# AI Usage Reflection — CS6650 Assignment 2

## 1. Overview

**AI Tool:** OpenAI ChatGPT  
**Project:** CS6650 Assignment 2 — Message Distribution and the Real Leaderboard

I used ChatGPT as a technical learning assistant, architecture discussion partner, and debugging aid while developing this distributed system.

My use of AI focused on understanding system behavior, evaluating concurrency and reliability trade-offs, interpreting test results, and reviewing implementation strategies. I did not treat AI-generated explanations as automatically correct. I questioned recommendations, compared them with assignment requirements, and used testing evidence to evaluate their validity.

This reflection distinguishes actual discussions (summarized in English) from retrospective technical analysis written after implementation. The summaries are not verbatim chat transcripts.

## 2. Areas Where AI Was Used

| Area | Purpose |
|---|---|
| System architecture | Understand the data flow between Ingest, RabbitMQ, Consumer, SQLite, and WebSocket |
| RabbitMQ | Discuss connection pooling, channel ownership, message acknowledgment, and recovery |
| Concurrency | Reason about consumer threading, per-resort ordering, and shutdown behavior |
| Reliability | Evaluate idempotency and at-least-once delivery guarantees |
| WebSocket | Investigate connection locality, message delivery, and push latency |
| Performance | Interpret 500K request results, queue backlog, and SQLite persistence |
| AWS deployment | Troubleshoot EC2, ALB, service configurations, and instance consistency |

## 3. Selected Technical Discussions

### Question #3 — Why Should Idempotency Be Tested Separately?

**Category:** Actual discussion, summarized in English.

**My question:**

"I do not want to skip idempotency validation. I only want to avoid repeating infrastructure startup and health checks unnecessarily at this point."

**Technical reasoning:**

RabbitMQ consumers using at-least-once delivery may receive a message more than once, for example when a consumer processes a message but disconnects before its acknowledgment reaches the broker.

Therefore, successful HTTP requests alone do not establish exactly-once aggregation.

I wanted a focused test to ensure that two deliveries with the same event ID resulted in only one database aggregation.

**My decision:** I rejected skipping the idempotency test merely because infrastructure health checks had already passed.

**Validation:** Two HTTP requests using the same `X-Event-ID` returned HTTP 201, while the database contained one corresponding processed-event record.

**What I learned:** Testing infrastructure availability and testing application correctness are separate responsibilities.

### Question #4 — Why Was the Consumer Shutdown Test Omitted?

**Category:** Actual discussion, summarized in English.

**My question:**

"Why was `ResortConsumerShutdownTest` omitted? Shouldn't it be run in addition to `TaskAdmissionGateTest`?"

**Technical reasoning:**

A shutdown mechanism must address multiple possible concurrency problems.

`TaskAdmissionGateTest` can verify whether new work is rejected after shutdown begins. However, that alone does not prove that previously accepted tasks finish safely or that the consumer stops without races.

A more comprehensive shutdown test should consider in-flight tasks, acknowledgments, executor termination, and resource cleanup.

**My decision:** I challenged the incomplete test plan and insisted that consumer shutdown behavior be evaluated separately.

**Verification status:** The project includes shutdown-related tests, but a successful independent shutdown-race execution must be documented before claiming the complete scenario passed.

**What I learned:** Passing one concurrency test does not necessarily establish correctness across an entire service lifecycle.

### Question #5 — Does Increasing WebSocket Drain Time Fix the Problem?

**Category:** Actual discussion, summarized in English.

**My question:**

"Is increasing the WebSocket drain timeout really required by the assignment? Would it fix the underlying issue, or only make the client wait longer?"

**Technical reasoning:**

The WebSocket drain period keeps subscriptions open after HTTP traffic ends, allowing delayed notifications to arrive.

Increasing this interval may increase the number of observed updates, but does not necessarily resolve slow consumer processing, delayed broadcasts, or inefficient notification delivery.

The assignment requires real leaderboard notifications, not merely a longer waiting period.

**My decision:** I questioned treating a longer timeout as a system-level performance fix. I retained the drain period as an observation mechanism and analyzed push latency separately.

**Validation:** In the single-ingest 500K run, 401,949 updates were observed after the 120-second drain, with WebSocket P95 push latency of 77,917 ms.

**What I learned:** A measurement workaround should not be confused with correcting the underlying bottleneck.

## 4. Retrospective Technical Analysis

The following questions were written after implementation to document the technical reasoning and trade-offs I studied. They are not presented as verbatim historical AI conversations.

### Question #7 — Why Use a RabbitMQ Channel Pool?

RabbitMQ channels should not be shared concurrently by multiple publishing threads.

Opening a new channel for every HTTP request would introduce avoidable overhead and resource consumption. A channel pool allows threads to borrow and return channels while maintaining exclusive ownership during publishing.

**Design rationale:** Use reusable connections and a bounded channel pool instead of opening a channel for every operation.

**Critical consideration:** Pool size, acquisition blocking, channel invalidation, and connection recovery can influence request throughput and failure behavior.

### Question #8 — Does HTTP 201 Guarantee Message Persistence?

HTTP 201 indicates that the server accepted a request according to its response logic. It does not automatically prove that the consumer has persisted the event.

Publishing to RabbitMQ, receiving a broker confirmation, consuming the message, and committing the database transaction are distinct stages.

**Critical distinction:** A positive HTTP response must not be treated as evidence that the complete asynchronous pipeline has finished.

**Verification approach:** Compare successful client requests with the eventual SQLite event count and RabbitMQ queue state.

### Question #10 — How Should At-Least-Once Delivery Interact with SQLite Idempotency?

At-least-once delivery supports retry and recovery but permits duplicate delivery.

The consumer must therefore identify previously processed event IDs and avoid applying their vertical totals again.

**Design rationale:** Persistent deduplication is preferable to relying only on an in-memory set because it can survive consumer restarts.

**Critical consideration:** Deduplication and aggregation must be coordinated transactionally so that a crash cannot leave an event marked processed without its corresponding state update, or vice versa.

### Question #12 — Why Is a WebSocket Backplane Necessary?

A WebSocket connection belongs to a particular server process, while the consumer calculating the leaderboard may execute elsewhere.

The consumer cannot assume that a subscriber is locally connected.

**Design rationale:** Separate the event-processing service from WebSocket connection management and use a messaging backplane to distribute leaderboard updates.

**Critical consideration:** The notification path must deliver updates to the server holding each relevant subscription. Connection routing, message ordering, and broadcast overhead all affect correctness and scalability.

### Question #13 — How Can We Verify a 500K Load Test Beyond HTTP Success?

A successful HTTP load test provides useful throughput and latency measurements, but does not independently prove that every event was processed.

**Validation method:**

1. Confirm the HTTP status distribution and expected request count.
2. Record the SQLite `processed_events` count before and after the test.
3. Confirm that RabbitMQ business queues eventually drain.
4. Check for unexpected increases in the dead-letter queue.
5. Evaluate WebSocket delivery and push latency independently.

**Observed result:** The two-ingest 500K test returned 500,000 successful HTTP 201 responses. SQLite increased from 13,027 to 513,027 processed events, and the RabbitMQ business queues eventually drained.

**What I learned:** Distributed correctness must be demonstrated across system boundaries, not inferred from a single API response.

## 5. Suggestions I Accepted, Modified, or Rejected

| AI-related recommendation | My response | Reason |
|---|---|---|
| Use pooled RabbitMQ channels | Accepted as an implementation approach | Avoid per-request channel creation and unsafe concurrent channel sharing |
| Use event-ID-based deduplication | Accepted and tested | Prevent duplicate aggregation under redelivery |
| Skip or defer focused idempotency validation | Rejected | Infrastructure health does not establish duplicate-processing correctness |
| Treat a longer WebSocket drain period as the solution | Challenged | More waiting does not remove processing or delivery bottlenecks |
| Use a limited shutdown test set | Corrected | Shutdown admission and in-flight termination require separate verification |
| Validate 500K through multiple system layers | Accepted | HTTP 201, RabbitMQ state, and SQLite persistence measure different stages |

## 6. An Example of Critical Thinking

One of the most important lessons from working with AI was that a technically plausible suggestion is not necessarily sufficient for a distributed system.

For example, increasing WebSocket drain time could produce a larger observed update count, but it would not necessarily improve real-time delivery.

Similarly, HTTP 201 responses could all succeed while messages remained queued and unprocessed.

I therefore emphasized evidence-based validation rather than assuming correctness from a successful API response or a single passing test.

## 7. Limitations and Transparency

This document records technical reasoning and decisions associated with my AI-assisted development process.

The actual-discussion section contains English paraphrases rather than complete verbatim transcripts. The retrospective section contains reconstructed study questions and technical analysis, not conversations claimed to have occurred.

I have not treated unverified tests as passed or invented missing experimental results.

I remain responsible for reviewing the code, understanding the implementation, validating system behavior, and explaining the engineering decisions included in this assignment.

## 8. Repository

https://github.com/LinCai-prog/CS6650-Assignment2
