defmodule Technician do
  @moduledoc false
  use GenServer
  use AMQP

  def start_link(spec_list = [_, _], name) do
    GenServer.start_link(__MODULE__, [spec_list, name], [])
  end

  @logs_exchange  "hospital_logs"
  @exam_exchange  "hospital_examinations"

  @logs           "info"

  def init([spec_list, name]) do
    {:ok, channel} = rabbitmq_connect(spec_list, name)
    {:ok, [channel, spec_list, name]}
  end

  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  def handle_info({
      :basic_deliver, payload,
      meta = %{reply_to: reply_to, correlation_id: correlation_id}
    }, state = [channel, _, name]) do

    IO.puts("#{inspect(name)} [x] Received [#{meta.routing_key}] #{payload}")
    payload
    |> to_charlist
    |> Enum.count(fn x -> x == ?. end)
    |> Kernel.*(1000)
    |> :timer.sleep()

    Basic.publish(channel, "", reply_to,
      "Examination for: #{payload}, type: #{meta.routing_key}, DONE!", correlation_id: correlation_id)

    Basic.publish(channel, @exam_exchange, "log.#{payload}",
      "Examination for: #{payload}, type: #{meta.routing_key}, DONE!", correlation_id: correlation_id)

    Basic.ack(channel, meta.delivery_tag)
    {:noreply, state}
  end

  def handle_info({:DOWN, _, :process, _pid, _reason}, [_, spec_list, name]) do
    {:ok, channel} = rabbitmq_connect(spec_list, name)
    {:noreply, [channel, spec_list, name]}
  end

  defp simple_receiver(parent) do
    receive do
      {:basic_deliver, payload, meta} ->
        IO.puts("#{inspect(parent)} [x] Received [#{meta.routing_key}] #{payload}")
        simple_receiver(parent)
    end
  end

  defp rabbitmq_connect(spec_list, name) do
    case Connection.open(heartbeat: 5) do
      {:ok, connection} ->
        Process.monitor(connection.pid)
        {:ok, channel} = Channel.open(connection)
        Basic.qos(channel, prefetch_count: 1)
        {:ok, channel}

        :ok = Exchange.declare(channel, @logs_exchange, :direct)
        :ok = Exchange.declare(channel, @exam_exchange, :topic)

        {:ok, %{queue: queue}} = Queue.declare(channel, "", exclusive: true)

        Queue.bind(channel, queue, @logs_exchange, routing_key: @logs)
        logs_receiver = spawn fn -> simple_receiver(name) end
        Basic.consume(channel, queue, logs_receiver, no_ack: true)

        for spec <- spec_list do
          {:ok, %{queue: queue_name}} = Queue.declare(channel, spec)
          Queue.bind(channel, queue_name, @exam_exchange, routing_key: spec)
          Basic.consume(channel, queue_name)
        end
        {:ok, channel}
      {:error, _} ->
        IO.puts("Reconnecting...")
        :timer.sleep(10000)
        rabbitmq_connect(spec_list, name)
    end
  end

end
