# Distributed Message Queue (Kafka-like System) in Python

## Overview

This project is a simplified, from-scratch implementation of a distributed message queue inspired by systems like Apache Kafka. It demonstrates core concepts of distributed systems, including producer-consumer architecture, message persistence, offset tracking, concurrency handling, and partition-based scaling.

The system is built using low-level Python modules such as `socket`, `threading`, and file I/O, without relying on external frameworks.

---

## Features

- TCP-based communication between clients and server
- Producer → Queue → Consumer architecture
- In-memory and disk-based message persistence
- FIFO message handling within partitions
- Multiple consumers with concurrency control
- Acknowledgment (ACK) mechanism for reliable delivery
- Offset tracking per consumer
- Offset persistence across restarts
- Log-based storage (append-only)
- Partitioning for parallel processing and scalability

---

## Architecture

### High-Level Flow

Producer → Server → Partitioned Message Logs → Consumers

### Components

#### 1. Producer
- Sends messages to the server over TCP
- Messages are distributed across partitions using round-robin

#### 2. Server
- Accepts producer and consumer connections
- Stores messages in partitioned logs
- Persists messages to disk
- Handles consumer requests using offsets
- Ensures thread-safe operations using locks

#### 3. Consumer
- Reads messages from a specific partition
- Maintains its own offset
- Persists offset to disk for recovery
- Polls server for new messages

---

## Project Structure
