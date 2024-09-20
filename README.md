# PGoTask
PGoTask is a simple and flexible task scheduler for Go, with a very small footprint.
The only requisite is an available Postgres database.

## How it works
1. Create a Scheduler and provide a Postgres connection
  - You will provide callbacks for specific task types; think of it as the classic queue topics
2. The Scheduler automatically creates tables for itself
3. The Scheduler runs in background and checks if new tasks are available on its table
  - It's possible to specify how much to wait between loops
4. During the dispatch phase, the pending tasks will be sent to the corresponding callbacks
  - In an error occurs, the task is re-scheduled (you can specify the cooldown) and the error is saved in a dedicated table
  - To guarantee operation integrity, you can use the same transaction as the Scheduler

## Concurrency
PGoTask is "concurrency-safe"; it's designed to be used from different services at the same time.
__Every dispatch applies a table lock__, so you can be sure that there won't be another Scheduler handling the same tasks as you.

## Task scheduling
It's possible to schedule tasks from the Scheduler itself _and_ "manually", via querying the table directly.
In this way, the system is very flexible:
- The service can schedule tasks indipendently
- It's possible to create task for specific deploys or routines, via migration scripts
- In extreme cases, you can create tasks manually, by querying your db
