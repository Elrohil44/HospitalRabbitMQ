defmodule Admin do
  @moduledoc false

  defp wait_for_message do
    receive do
      {:basic_deliver, payload, meta} ->
        IO.puts(" [x] Received [#{meta.routing_key}] #{payload}")
        wait_for_message()
    end
  end

  defp loop(channel) do
    info = IO.gets "INFO:\t"
    info = info |> String.split(["\r", "\n"]) |> List.first
    AMQP.Basic.publish(channel, "hospital_logs", "info", info)
    loop(channel)
  end

  def start do
    receiver = spawn fn -> wait_for_message() end

    {:ok, connection} = AMQP.Connection.open(heartbeat: 5)
    {:ok, channel} = AMQP.Channel.open(connection)

    :ok = AMQP.Exchange.declare(channel, "hospital_logs", :direct)
    :ok = AMQP.Exchange.declare(channel, "hospital_examinations", :topic)

    {:ok, %{queue: queue_name}} = AMQP.Queue.declare(channel, "", exclusive: true)

    AMQP.Queue.bind(channel, queue_name, "hospital_examinations", routing_key: "#")
    AMQP.Basic.consume(channel, queue_name, receiver, no_ack: true)

    loop(channel)
  end
end
