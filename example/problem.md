## The Problem: The "Titan" Supercomputer Scheduler

**Scenario:**
The university has a single supercomputer cluster, "Titan." Hundreds of students, researchers, and professors submit computationally heavy tasks (simulations, AI training, data rendering) via a web portal. The system must accept a standardized JSON payload, place it in a queue, and execute jobs reliably without crashing the mainframe or unfairly locking out users.

### 1. The Baseline Requirements (Your Foundation)

* **Standardized Ingestion:** Jobs are submitted as JSON containing metadata (user ID, department, job type) and a pointer to the dataset.
* **Priority Tiering:** Professors (Tier 1) jump ahead of Post-Docs (Tier 2), who jump ahead of Undergrads (Tier 3).
* **Failure & Retry:** If a job fails (e.g., memory overflow, division by zero), it is appended to the *end* of its priority queue to try again later.

### 2. Advanced Requirements (The Stress Test)

To prove you understand enterprise queueing, your design should solve these additional challenges:

* **Starvation Prevention (Job Aging):** If professors continuously submit jobs, an undergrad's job might sit in the queue forever. **Requirement:** Implement "aging." Every 12 hours a job sits in the queue, its priority temporarily bumps up one tier so it eventually runs.
* **Dead-Letter Queues (DLQ):** If a job is fundamentally broken (e.g., a syntax error in the math), it will fail infinitely, clogging the queue. **Requirement:** After 3 failed retries, the job is removed from the main queue and routed to a DLQ for manual inspection, and the user is alerted.
* **Timeouts & TTL (Time-To-Live):** A user submits an infinite loop. **Requirement:** Every job must declare an expected maximum runtime in its JSON. If the worker processes the job longer than this time, the queue must forcefully terminate the worker's current task and fail the job.
* **Fairness / Resource Limits:** Professor Smith submits 10,000 jobs at once. Professor Jones submits 1 job. **Requirement:** The queue must use Round-Robin or active throttling to ensure Jones doesn't have to wait for all 10,000 of Smith's jobs to finish, even though they are the same priority.
* **Job Cancellation:** A student realizes their JSON parameters are wrong and wants to cancel a job. **Requirement:** The system must be able to intercept and remove a job from the queue before it runs, or gracefully kill it if it has already been picked up by a worker.

---

## 3. The Architecture to Build

To implement this, you will need to design three distinct components:

1. **The Producer (API Gateway):** A lightweight web server that receives the JSON, validates it (is the payload too large?), and publishes it to the queue.
2. **The Message Broker:** The actual queueing infrastructure (e.g., RabbitMQ, Redis, Amazon SQS). This handles the sorting, priority routing, and persistence (so if the queue server reboots, jobs aren't lost).
3. **The Consumers (Worker Nodes):** Background processes that listen to the queue, pull the next job, execute the calculation, and report success/failure back to the broker.

---

## 4. Test Cases to Prove Your Design

When you build or present this, run these specific scenarios to prove the queue is robust:

* **The Poison Pill:** Submit a job with a malformed payload that instantly crashes the worker. *Result should be:* The queue realizes the worker died, reassigns the job to a new worker. After 3 crashes, it routes to the DLQ.
* **The Big Squeeze:** Submit 50 low-priority jobs. While they are processing, submit 5 high-priority jobs. *Result should be:* The workers finish their *current* low-priority jobs, then immediately pivot to the high-priority jobs before finishing the remaining low-priority ones.
* **The Long Haul:** Submit a job that takes 10 minutes to run, but shut down the worker node at minute 5 (simulating a power outage). *Result should be:* The queue's acknowledgement system detects the lost connection, safely returns the job to the queue, and another worker picks it up from scratch.
