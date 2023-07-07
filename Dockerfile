# Use an official Golang runtime as the base image
FROM golang:1.17-alpine

# Install Git
RUN apk update && apk add --no-cache git

# Set the working directory inside the container
WORKDIR /app

# Copy the source code to the working directory
COPY . .

# Download and cache Go modules
RUN go mod download

# Add the missing module explicitly (using a compatible version)
RUN go get github.com/Shopify/toxiproxy/v2@v2.5.0

# Build the Go application
RUN go build -o consumer ./src/main.go

# Set environment variables for Kafka configuration
ENV KAFKA_SERVER=localhost:9092
ENV KAFKA_PARTITION=0
ENV KAFKA_TOPIC=ecro_req_topic
ENV CONSUMER_PROPERTIES_FILE=/app/config/consumer.properties

# Expose any necessary ports
EXPOSE 8080

# Run the Go application
CMD ["./consumer"]
