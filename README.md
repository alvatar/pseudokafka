# Pseudo-Kafka

_Even more Kafkian than Kafka._


## What is this about?

It's just a simple project that aims at providing an in-memory replacement for Kafka, with two primary purposes:

1. Provide a light-weight Kafka server for the development process. So you don't need to run and configure your own copy of Kafka locally, while you only want to start sending and receiving some messages.

2. For testing. Probably better suited for integration tests, if you don't want to run the full Kafka service just to send some messages.

## Features

Keep in mind that this project is under development, and currently only aims at supporting the basic functionality:

- ApiVersions/Metadata
- Create/Delete Topics
- Produce/Consume Messages

Note that this basically it leaves behind all the functionality related to the distributed systems behaviour, for obvious reasons. But if this messes up with some of your code, please let me know. If you need to mock some of this behaviour, please open an Issue (or better a Pull Request).
