namespace Streamlinr

module RuntimeTests =
    open System
    open System.Threading
    open System.Threading.Tasks
    open Xunit

    let private waitForCompletion (task: Task<'T>) =
        Assert.True(task.Wait(TimeSpan.FromSeconds 2.0), "Timed out waiting for actor response.")
        task.Result

    [<Fact>]
    let ``processor actor processes a source record batch`` () =
        let processed = TaskCompletionSource<SourceRecord>(TaskCreationOptions.RunContinuationsAsynchronously)
        let processor =
            RecordProcessor(fun record _ ->
                processed.SetResult record
                Task.CompletedTask)
        let plan = ProcessorPlan("peek-1", [| "source-1" |], processor)
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = SourceRecord("orders", 0, 1L, "order-1", "created", Nullable())
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
        let plan = ProcessorPlan("peek-1", [| "source-1" |], processor)
        let actor = ProcessorActor.start plan CancellationToken.None
        let record = SourceRecord("orders", 0, 1L, "order-1", "created", Nullable())
        let batch = SourceRecordBatch(Guid.NewGuid(), "source-1", [| record |], DateTimeOffset.UtcNow)

        let result = Actor.postAndAsyncReply (fun reply -> ProcessBatch(batch, reply)) actor |> Async.StartAsTask |> waitForCompletion

        match result with
        | Ok _ -> Assert.Fail("Expected processor failure.")
        | Error error ->
            Assert.Equal("peek-1", error.ProcessorId)
            Assert.Contains("callback failed", error.Error.Message)
