namespace Streamlinr

module RuntimeTests =
    open System
    open System.Threading
    open System.Threading.Tasks
    open Xunit

    let private waitForCompletion (task: Task<'T>) =
        Assert.True(task.Wait(TimeSpan.FromSeconds 2.0), "Timed out waiting for actor response.")
        task.Result

    let private deadLetterFactory =
        RuntimeDeadLetterFactory(fun record _ -> record.Value)

    let private processorPlan processor =
        ProcessorPlan("peek-1", [| "source-1" |], processor, RuntimeProcessorFailureAction.FailTopology, deadLetterFactory)

    let private processorPlanWithFailureAction failureAction processor =
        ProcessorPlan("peek-1", [| "source-1" |], processor, failureAction, deadLetterFactory)

    let private sourceRecord () =
        SourceRecord("orders", 0, 1L, "order-1", "created", Array.empty<RuntimeHeader>, Nullable())

    [<Fact>]
    let ``processor actor processes a source record batch`` () =
        let processed = TaskCompletionSource<SourceRecord>(TaskCreationOptions.RunContinuationsAsynchronously)
        let processor =
            RecordProcessor(fun record _ ->
                processed.SetResult record
                Task.CompletedTask)
        let plan = processorPlan processor
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = sourceRecord ()
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        Assert.True(Result.isOk result)
        Assert.Equal("order-1", (waitForCompletion processed.Task).Key :?> string)

    [<Fact>]
    let ``processor actor reports callback failure`` () =
        let processor =
            RecordProcessor(fun _ _ ->
                raise (InvalidOperationException("callback failed"))
                Task.CompletedTask)
        let plan = processorPlan processor
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = sourceRecord ()
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        match result with
        | Ok _ -> Assert.Fail("Expected processor failure.")
        | Error error ->
            Assert.Equal("peek-1", error.ProcessorId)
            Assert.Equal("source-1", error.SourceId)
            Assert.Equal(RuntimeProcessorFailureAction.FailTopology, error.FailureAction)
            Assert.Same(deadLetterFactory, error.DeadLetterFactory)
            Assert.Contains("callback failed", error.Error.Message)

    [<Fact>]
    let ``processor actor reports skip failure action`` () =
        let processor =
            RecordProcessor(fun _ _ ->
                raise (InvalidOperationException("callback failed"))
                Task.CompletedTask)
        let plan = processorPlanWithFailureAction RuntimeProcessorFailureAction.Skip processor
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = sourceRecord ()
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        match result with
        | Ok _ -> Assert.Fail("Expected processor failure.")
        | Error error ->
            Assert.Equal(RuntimeProcessorFailureAction.Skip, error.FailureAction)
            Assert.Same(record, error.Record)

    [<Fact>]
    let ``processor actor reports continue as dead letter failure action`` () =
        let processor =
            RecordProcessor(fun _ _ ->
                raise (InvalidOperationException("callback failed"))
                Task.CompletedTask)
        let plan = processorPlanWithFailureAction RuntimeProcessorFailureAction.ContinueAsDeadLetter processor
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = sourceRecord ()
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        match result with
        | Ok _ -> Assert.Fail("Expected processor failure.")
        | Error error ->
            Assert.Equal(RuntimeProcessorFailureAction.ContinueAsDeadLetter, error.FailureAction)
            Assert.Same(record, error.Record)
            Assert.Equal("created", error.DeadLetterFactory.Invoke(record, error.Error) :?> string)

    [<Fact>]
    let ``processor actor reports pause partition failure action`` () =
        let processor =
            RecordProcessor(fun _ _ ->
                raise (InvalidOperationException("callback failed"))
                Task.CompletedTask)
        let plan = processorPlanWithFailureAction RuntimeProcessorFailureAction.PausePartition processor
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = sourceRecord ()
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        match result with
        | Ok _ -> Assert.Fail("Expected processor failure.")
        | Error error ->
            Assert.Equal(RuntimeProcessorFailureAction.PausePartition, error.FailureAction)
            Assert.Same(record, error.Record)
