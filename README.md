# Employee Commute Route Optimizer Consumer

The Employee Commute Route Optimizer (ECRO) Consumer is a Kafka consumer that receives route requests from the ECRO Producer, retrieves the client requests from ecro_req_topic to Route APIs, process the requests and forwards them to ecro_res_topic kafka topic.

## Installation

1. Clone the repository:

```shell
git clone https://github.com/muradshahsuvarov/employee-commute-route-optimizer-consumer
```

2. Install dependencies:

```shell
go mod download
```

## Usage

1. Start the ECRO Consumer:

```shell
go run src/main.go
```

2. The consumer will start listening for route requests on the specified Kafka topic.

3. When a route request is received, the consumer will retrieve the optimized route using the RouteFinder API and send the route response back to the Producer.

## Kafka Topic Configuration

The ECRO Consumer is configured to consume route requests from the `ecro_req_topic` Kafka topic by default. If you want to change the topic, modify the following line in the `main` function of the `main.go` file:

```go
ConsumeMessages("localhost:9092", 0, "ecro_req_topic")
```

Replace `"ecro_req_topic"` with the desired Kafka topic name.

## Contributing

Murad Shahsuvarov
