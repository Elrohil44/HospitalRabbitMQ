defmodule Doctor do
  @moduledoc false
  defp wait_for_message(channel, correlation_ids, receiver) do
    receive do
      {:basic_deliver, payload, meta = %{correlation_id: correlation_id}} ->
        handle_message(channel, correlation_ids, receiver, correlation_id in correlation_ids,
          correlation_id, {payload, meta})
      {:add_id, new_id} ->
        wait_for_message(channel, [new_id | correlation_ids], receiver)
    end
  end

  defp handle_message(channel, correlation_ids, receiver, true, correlation_id, msg) do
    send(receiver, msg)
    wait_for_message(channel, List.delete(correlation_ids, correlation_id), receiver)
  end

  defp handle_message(channel, correlation_ids, receiver, false, _, _) do
    wait_for_message(channel, correlation_ids, receiver)
  end

  defp wait_for_message(channel, receiver) do
    receive do
      {:basic_deliver, payload, meta} ->
      send(receiver, {"INFO: #{payload}", meta})
      wait_for_message(channel, receiver)
    end
  end

  defp receive_local do
    receive do
      {payload, meta} ->
        IO.puts(" [x] Received [#{meta.routing_key}] #{payload}")
        receive_local()
    end
  end

  defp loop(channel, receiver, reply_to) do
    type = IO.gets "Examination type:\t"
    type = type |> String.split(["\r", "\n"]) |> List.first
    name = IO.gets "Name:\t"
    name = name |> String.split(["\r", "\n"]) |> List.first

    correlation_id =
      :erlang.unique_integer
      |> :erlang.integer_to_binary
      |> Base.encode64

    AMQP.Basic.publish(channel,
                      "hospital_examinations",
                      type,
                      name,
                      reply_to: reply_to,
                      correlation_id: correlation_id)

    send(receiver, {:add_id, correlation_id})
    loop(channel, receiver, reply_to)
  end

  def start do
    receiver = spawn fn -> receive_local() end

    {:ok, connection} = AMQP.Connection.open(heartbeat: 5)
    {:ok, channel} = AMQP.Channel.open(connection)

    logs_consumer = spawn fn -> wait_for_message(channel, receiver) end
    examinations_consumer = spawn fn -> wait_for_message(channel, nil, receiver) end

    :ok = AMQP.Exchange.declare(channel, "hospital_logs", :direct)
    :ok = AMQP.Exchange.declare(channel, "hospital_examinations", :topic)

    {:ok, %{queue: queue_logs}} = AMQP.Queue.declare(channel, "", exclusive: true)
    {:ok, %{queue: queue_examine}} = AMQP.Queue.declare(channel, "", exclusive: true)

    AMQP.Queue.bind(channel, queue_logs, "hospital_logs", routing_key: "info")

    AMQP.Basic.consume(channel, queue_logs, logs_consumer, no_ack: true)
    AMQP.Basic.consume(channel, queue_examine, examinations_consumer, no_ack: true)

    loop(channel, examinations_consumer, queue_examine)
  end
end
