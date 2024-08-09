# arugula - Scala Redis client wrapper for lettuce

Arugula is a Redis client wrapper for Scala built on top of 
[Java lettuce](https://github.com/redis/lettuce).

## Why another Scala Redis client
There are already a number of Redis clients for Scala, including [scala-redis](https://github.com/debasishg/scala-redis) 
and [scredis](https://github.com/scredis/scredis), so you might be wondering:

Why create another one? 

Maintaining a redis client is a lot of work, and the existing clients are either poorly maintained
or have heavy dependencies like Akka or Cats-Effect. We were looking for a clean Scala API with 
minimal dependencies that would allow us to use the full feature set of Redis and that could be easily tested.

[Lettuce](https://github.com/redis/lettuce) is a well-maintained and feature-rich Redis client for Java. 
While it is possible to use the Java client directly in Scala,
Arugula provides a more idiomatic Scala API and is designed to be lightweight and easy to use.
Rather than trying to reinvent the wheel, Arugula leverages the work done by the [Lettuce](https://github.com/redis/lettuce) team 
and provides a more Scala-friendly API. 
All the commands map directly to the Lettuce commands, so the API is very similar to the Lettuce API.

## Features
The code is split up into two main packages:
- `arugula-api` - contains the pure scala API
- `arugula-core` - contains the bindings to Lettuce

The api allows you to create a Redis client and execute commands on it. 
The core package contains the implementation of the commands using Lettuce.
An implementation can be written independently of Lettuce, and the commands can be tested 
without a Redis Server or the Lettuce dependency.

## Usage
### SBT Configuration
To use Arugula, add the following to your `build.sbt` file:


```scala
val arugulaVersion = "0.0.1"

libraryDependencies += "com.github.scoquelin" %% "arugula-api" % arugulaVersion
libraryDependencies += "com.github.scoquelin" %% "arugula-core" % arugulaVersion
```

This will add the Arugula API and core packages to your project along with the Lettuce dependency.
If you want to only use the API in your package, you can exclude the core package from the dependencies
and code only against the API. This decouples the API from the core implementation and allows you to write
your own implementation of the commands or write an adapter for another underlying Redis client.

While the API is decoupled from any direct Lettuce dependencies, the API mirrors Lettuce pretty closely, 
so the documentation provided for Lettuce can provide a lot of guidance on how to use Arugula.


### Quick Start Example

Here is an example of how to use Arugula:

```scala
import com.github.scoquelin.arugula.connection.RedisConnection
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig


object Main extends App {
  import scala.concurrent.ExecutionContext.Implicits.global
  val client = LettuceRedisCommandsClient(LettuceRedisClientConfig(
    host = "localhost",
    port = 6379
  ))

  val key = "key"
  val value = "value"

  for {
    _ <- client.set(key, value)
    result <- client.get(key)
  } yield {
    println(result)
  }
}
```

## Configuration
The `LettuceRedisClientConfig` class allows you to configure the Redis client.
Here are the available options:

- `host` - The host of the Redis server. Default is `localhost`.
- `port` - The port of the Redis server. Default is `6379`.
- more to come...

## Testing
To run the tests, simply run `sbt test` in the root directory.
You can run the integration tests as well by running `sbt testAll`.
The integration tests use test containers to spin up a Redis server and run the tests against it
including a redis cluster.

## Roadmap
See the [open issues](https://github.com/scoquelin/arugula/issues) for a list of proposed features (and known issues).
Here are some of the features that are planned for the future:

- Add more configuration options, including password authentication or IAM authentication
- Add Full support for Redis commands supported by Lettuce
- Better documentation
- Standardized error handling


## Contributing
Contributions are welcome! Feel free to open an issue or submit a pull request.