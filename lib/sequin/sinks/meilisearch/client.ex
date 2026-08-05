defmodule Sequin.Sinks.Meilisearch.Client do
  @moduledoc """
  Client for interacting with the Meilisearch API.
  """

  alias Sequin.Consumers.MeilisearchSink
  alias Sequin.Error

  require Logger

  defp decode_body(body) when is_map(body), do: body

  defp decode_body(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> %{}
    end
  end

  defp decode_body(_), do: %{}

  # Meilisearch's task queue is SERIAL, so a task may sit "enqueued" for a while behind
  # others before it processes. We poll until it reaches a terminal state or this deadline,
  # rather than abandoning a task that is still legitimately in flight (which caused
  # "Task verification timed out" errors → nack → redeliver → no backfill progress).
  # Kept under the consumer's ack_wait_ms (default 30s) so we confirm delivery before the
  # message's visibility window expires. Set to 25s (under 30s) to tolerate deeper task
  # queues when batcher concurrency is high. Overridable via config for tests.
  @default_task_wait_timeout_ms 25_000
  @task_poll_base_ms 100
  @task_poll_max_ms 1_000

  defp task_wait_timeout_ms do
    Application.get_env(:sequin, :meilisearch, [])[:task_wait_timeout_ms] || @default_task_wait_timeout_ms
  end

  defp wait_for_task(%MeilisearchSink{} = sink, task_id) do
    wait_for_task(sink, task_id, 0, 0)
  end

  defp wait_for_task(%MeilisearchSink{} = sink, task_id, elapsed_ms, attempt) do
    req =
      sink
      |> base_request()
      |> Req.merge(url: "/tasks/#{task_id}", retry: false)

    case Req.get(req) do
      {:ok, %{body: body}} ->
        case decode_body(body) do
          %{"status" => status} when status in ["succeeded", "success"] ->
            :ok

          %{"status" => "failed", "error" => error} ->
            message = extract_error_message(error)
            {:error, Error.service(service: :meilisearch, message: message, details: error)}

          %{"status" => status} when status in ["enqueued", "processing"] ->
            maybe_poll_again(sink, task_id, elapsed_ms, attempt, status)

          _ ->
            {:error, Error.service(service: :meilisearch, message: "Invalid response format")}
        end

      {:error, reason} ->
        # Transient transport error — keep polling within the deadline before giving up.
        maybe_poll_again(sink, task_id, elapsed_ms, attempt, {:transport_error, reason})
    end
  end

  defp maybe_poll_again(sink, task_id, elapsed_ms, attempt, last) do
    if elapsed_ms >= task_wait_timeout_ms() do
      case last do
        {:transport_error, reason} ->
          {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}

        status ->
          {:error,
           Error.service(
             service: :meilisearch,
             message: "Task verification timed out",
             details: %{task_id: task_id, last_status: status}
           )}
      end
    else
      delay = min(@task_poll_max_ms, trunc(@task_poll_base_ms * :math.pow(2, attempt)))

      Logger.debug("[Meilisearch] Task #{task_id} not terminal (#{inspect(last)}), polling again in #{delay}ms")

      Process.sleep(delay)
      wait_for_task(sink, task_id, elapsed_ms + delay, attempt + 1)
    end
  end

  @doc """
  Import multiple documents in JSONL format.
  """
  def import_documents(%MeilisearchSink{} = sink, index_name, records) do
    jsonl = Enum.map_join(records, "\n", &Jason.encode!/1)

    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents",
        headers: [{"Content-Type", "application/x-ndjson"}],
        body: jsonl
      )

    case Req.post(req) do
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Transport error: #{Exception.message(error)}"
         )}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Delete documents from an index.
  """
  def delete_documents(%MeilisearchSink{} = sink, index_name, document_ids) do
    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/delete-batch",
        body: Jason.encode!(document_ids),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Transport error: #{Exception.message(error)}"
         )}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Update documents using a function expression.
  """
  def update_documents_with_function(%MeilisearchSink{} = sink, index_name, filter, function, context \\ %{}) do
    body = %{
      "filter" => filter,
      "function" => function
    }

    body = if map_size(context) > 0, do: Map.put(body, "context", context), else: body

    req =
      sink
      |> base_request()
      |> Req.merge(
        url: "/indexes/#{index_name}/documents/edit",
        body: Jason.encode!(body),
        headers: [{"Content-Type", "application/json"}]
      )

    case Req.post(req) do
      {:ok, %{body: %{"taskUid" => task_id}}} ->
        wait_for_task(sink, task_id)

      {:ok, %{status: status, body: body}} ->
        message = extract_error_message(body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: %{status: status, body: body}
         )}

      {:error, %Req.TransportError{} = error} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Transport error: #{Exception.message(error)}"
         )}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Get information about an index.
  """
  def get_index(%MeilisearchSink{} = sink, index_name) do
    req = base_request(sink)

    case Req.get(req, url: "/indexes/#{index_name}") do
      {:ok, %{status: status, body: body}} when status == 200 ->
        decoded_body = decode_body(body)
        {:ok, decoded_body["primaryKey"]}

      {:ok, %{body: body}} ->
        decoded_body = decode_body(body)
        message = extract_error_message(decoded_body)
        {:error, Error.service(service: :meilisearch, message: message, details: decoded_body)}

      {:error, reason} ->
        {:error, Error.service(service: :meilisearch, message: "Unknown error", details: reason)}
    end
  end

  @doc """
  Test the connection to the Meilisearch server.
  """
  def test_connection(%MeilisearchSink{} = sink) do
    req = base_request(sink)

    case Req.get(req, url: "/health") do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: status, body: body}} ->
        decoded_body = decode_body(body)
        message = extract_error_message(decoded_body) || "Request failed with status #{status}"

        {:error,
         Error.service(
           service: :meilisearch,
           message: message,
           details: decoded_body
         )}

      {:error, reason} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: "Cannot connect to Meilisearch",
           details: reason
         )}
    end
  end

  def maybe_verify_index(%MeilisearchSink{index_name: nil}, _index_name, _primary_key) do
    :ok
  end

  def maybe_verify_index(%MeilisearchSink{} = sink, index_name, primary_key) do
    case get_index(sink, index_name) do
      {:ok, ^primary_key} ->
        :ok

      {:ok, other_primary_key} ->
        {:error,
         Error.service(
           service: :meilisearch,
           message: ~s(Index verification failed. Expected primary key "#{primary_key}", got "#{other_primary_key}")
         )}

      {:error, error} ->
        {:error, Error.service(service: :meilisearch, message: "Index verification failed", details: error)}
    end
  end

  # Private helpers

  defp default_req_opts do
    Application.get_env(:sequin, :meilisearch, [])[:req_opts] || []
  end

  defp base_request(%MeilisearchSink{} = sink) do
    [
      base_url: String.trim_trailing(sink.endpoint_url, "/"),
      headers: [{"Authorization", "Bearer #{sink.api_key}"}],
      receive_timeout: to_timeout(second: sink.timeout_seconds),
      retry: :transient,
      retry_delay: fn retry_count ->
        Sequin.Time.exponential_backoff(500, retry_count, 5_000)
      end,
      max_retries: 1,
      compress_body: true
    ]
    |> Req.new()
    |> Req.merge(default_req_opts())
  end

  defp extract_error_message(error) do
    cond do
      is_binary(error["message"]) -> error["message"]
      is_binary(error["code"]) -> error["code"]
      true -> nil
    end
  end
end
