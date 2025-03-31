# What?

This is a reference implementation of a **PostgreSQL-based Message Queue for
Elixir**.

# Why?

Great question! Phoenix already has a [fantastic Pub/Sub
system](https://hexdocs.pm/phoenix_pubsub), right? And Elixir already has great
built-in messaging primitives. But these options do not offer **durability**. In
other words, if something fails, the message will be lost. There are no
**guarantees** that the message will be delivered and processed as intended.

But we have [Oban](https://hexdocs.pm/oban) for that, right? Well, yes and no.
Oban is primarily a _job framework_, and the "job" concept it models is pretty
heavy, with each type of job requiring its own module. Elixir's messaging is far
more lightweight. Can we find something more like Phoenix Pub/Sub, but with
delivery guarantees?

What you need is a **message queue**. And the obvious choice (especially within
the BEAM ecosystem) is [RabbitMQ](https://www.rabbitmq.com/), coupled with
[Broadway](https://hexdocs.pm/broadway/rabbitmq.html). And this is a genuinely
fantastic combo, which you can see an example of in [my CQRS example
repo](https://github.com/giddie/elixir_cqrs_example). But it requires that you
deploy RabbitMQ (or whatever queue/broker you choose) alongside your
application, which adds complexity to your deployment.

Wouldn't it be great if we could have the benefits of a **simple, lightweight
message queue** like the RabbitMQ/Broadway combo, but use **only PostgreSQL**?
Yep, that's what we have here.

# How?

In a nutshell, a database table is used to store all messages, with a `queue`
column allowing us to maintain separate queues. Queue "processors" can then pull
messages out of these queues and deliver them according to a specified routing
configuration.

The core functionality is in the [`Messaging`](/lib/messaging.ex) module, which
is where you'll find functions to **broadcast messages** and **process
messages** from a queue:

```elixir
iex> Repo.transaction(fn ->
...>   [%Message{type: "Example.Event", schema_version: 1, payload: %{"one" => 1}}]
...>   |> Messaging.broadcast_messages!(to_queue: "my_queue")
...> end)
{:ok, :ok}

iex> Messaging.process_message_queue_batch("my_queue")
[info] Messaging: %PostgresqlMessageQueue.Messaging.Message{type: "Example.Event", schema_version: 1, payload: %{}, metadata: %{}}
1
```

But we don't want to **process message batches manually**, of course, so we use
a [`MessageQueueProcessor`](/lib/messaging/message_queue_processor.ex) instead:

```elixir
iex> Messaging.MessageQueueProcessor.start_link(queue: "my_queue")
{:ok, #PID<0.251.0>}
[info] Messaging: %PostgresqlMessageQueue.Messaging.Message{type: "Example.Event", schema_version: 1, payload: %{}, metadata: %{}}
[info] Messaging.MessageQueueWatcher [#PID<0.326.0>] Subscribing queue: my_queue. Already subscribed: global.
```

This server uses [Broadway](https://hexdocs.pm/broadway) under the hood, via a
custom [Broadway Producer](/lib/messaging/message_queue_broadway_producer.ex).

What's this [`MessageQueueWatcher`](/lib/messaging/message_queue_watcher.ex)?
It's a GenServer that leverages [PostgreSQL's LISTEN
directive](https://www.postgresql.org/docs/current/sql-listen.html) to **wait
for a new message** to arrive in the queue. That way we can avoid constantly
**polling the database**. (Actually it does this using a generic
[`NotificationListener`](/lib/persistence/notification_listener.ex) server,
which can subscribe any number of processes to whatever topics they want to
listen to, all on a **single database connection**.)

OK, but how do we **consume these messages**? The simplest, default approach is
via application config:

```elixir
config :postgresql_message_queue, PostgresqlMessageQueue.Messaging,
  broadcast_listeners: [
    {MyContext.MyMessageHandler, ["MyContext.Commands.*", "AnotherContext.Events.*"]},
    {MyLogger.EventLogger, ["*.Events.*"]}
  ]
```

This configuration allows you to specify a kind of **static routing table** for
your messages. Each listed module in the config must implement the
`Messaging.MessageHandler` behaviour, which requires a single `handle_message/1`
function. And then we list the message "types" the module is interested in. As
you can see, **wildcards** are supported, which allows you to fire all kinds of
exotic messages at a message handler with very little boilerplate.

Here's an example from the [ExampleUsage](/lib/example_usage.ex) module:

```elixir
@impl Messaging.MessageHandler
def handle_message(%Messaging.Message{
      type: "ExampleUsage.Events.Greeting",
      payload: %{"greeting" => greeting}
    }) do
  Logger.info("ExampleUsage: received greeting: #{greeting}")
end
```

# Handling Failures

If a message fails, by default it will be retried instantly. You can change this
behaviour by passing a `backoff_ms` option to `MessageQueueProcessor`,
containing a function that defines your backoff curve. Here's a simple example:

```elixir
backoff_ms = fn attempt when is_integer(attempt) ->
  base = 2 ** (attempt - 1) * 5 - 5
  jitter = Enum.random(-base..base) |> Integer.floor_div(20)
  base + jitter
end
```

See it in context in the
[Application](/lib/postgresql_message_broker/application.ex) module.

# Can I delay a message for later processing?

Yes! Use `process_after` when broadcasting the message:

```elixir
[%Message{type: "Example.Event", schema_version: 1, payload: %{"one" => 1}}]
|> Messaging.broadcast_messages!(to_queue: "my_queue", process_after: {5, :minute})
{:ok, :ok}
```

Any unit supported by `DateTime.add/4` is OK. You can also simply specify a
DateTime struct, if you prefer.

# Can I run more than one `MessageQueueProcessor`

Yeah, absolutely. In fact you _should_ do this if you need more than just a
simple global queue. Chances are you may need a few queues for, e.g. messages
that should be processed in strict order, whereas the global queue can be used
for general background processing where ordering doesn't matter. The global
queue can have a high `concurrency` value set to help throughput.

You can also have multiple nodes running `MessageQueueProcessors` for the same
queue. This works just fine because PostgreSQL handles row-level locking for the
messages. But bear in mind that if you do this, you cannot guarantee that
messages will be processed in order.

# Can I use a custom routing table?

Yes: you have a couple of options.

1. You can specify a `handler` opt for `MessageQueueProcessor`, which is a
   function that will receive all messages in that queue. And you can do your
   own custom routing.
2. You can use `Messaging.deliver_messages_to_handlers!/2` as the handler, and
   pass in a custom handler config:

```elixir
handler_configs = [
  {MyContext.MyMessageHandler, ["MyContext.Commands.*", "AnotherContext.Events.*"]},
  {MyLogger.EventLogger, ["*.Events.*"]}
]
handler = &Messaging.deliver_messages_to_handlers!([&1], handler_configs)
MessageQueueProcessor.start_link(queue: "my_queue", handler: handler)
```

# How do I use this in my project?

Right now, the best way is to copy the modules you're interested in, rename as
appropriate, and start the relevant GenServers:

* `Persistence.NotificationListener`
* `Messaging.MessageQueueWatcher`
* `Messaging.MessageQueueProcessor`

# Shouldn't this be a library?

Yeah, maybe. One thing that concerns me is that each project has different
needs. It's quite likely that this code will need a little work to adapt it for
your project. However, a library hides away the code, making it harder to adapt
and modify. Essentially, the library starts shaping how the app should work. And
that's not necessarily a good thing.

It might be possible to shape this into a library that is flexible enough for
any project, and I'm giving that some thought. But for now this is a code
reference to help anyone who wants to copy what is helpful and adapt as needed.
