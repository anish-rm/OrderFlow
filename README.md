# OrderFlow

OrderFlow is an event-driven order processing system built with .NET and Apache Kafka. It demonstrates how different microservices can collaborate through asynchronous events, with order placement, payment handling, inventory validation, and notifications flowing through Kafka topics.

## Overview

This project is designed to showcase a practical implementation of distributed messaging patterns in a .NET ecosystem. It includes separate services for:

- Orders API
- Payment processing
- Inventory reservation
- Notifications
- Shared contracts and domain models

## Architecture

The solution uses Kafka topics to propagate domain events such as:

- Order placed
- Payment received
- Inventory updates
- Notification triggers

The design follows an event-driven architecture and uses outbox-like patterns to help coordinate service interactions.

## Tech Stack

- .NET 8 / ASP.NET Core
- Kafka
- C#
- Entity Framework Core / relational storage
- Docker-friendly microservice setup

## Project Structure

- `OrderFlow.Application` - application services and DTOs
- `OrderFlow.Common` - shared helper types and constants
- `OrderFlow.Contracts` - Avro schemas and generated contract classes
- `OrderFlow.Domain` - domain entities and persistence contexts
- `OrderFlow.Inventory` - inventory consumer service
- `OrderFlow.Notification` - notification consumer service
- `OrderFlow.OrdersApi` - API entry point for placing orders
- `OrderFlow.Payments` - payment processing and relay service

## Getting Started

1. Clone the repository
2. Restore NuGet packages
3. Start Kafka and required infrastructure
4. Run the solution in Visual Studio or with `dotnet run`
5. Use the Orders API to create sample orders

## License

This project is provided for learning and demonstration purposes.
