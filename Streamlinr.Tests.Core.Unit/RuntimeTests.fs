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

    let private noOpProcessor =
        RecordProcessor(fun _ _ -> Task.CompletedTask)

    let private skipSinkEncoder =
        RuntimeSinkEncoder(fun _ -> RuntimeSinkResult.Skip())

    let private processorPlan processor =
        ProcessorPlan("peek-1", "source-1", [| "source-1" |], processor, RuntimeProcessorFailureAction.FailTopology, deadLetterFactory)

    let private processorPlanWithFailureAction failureAction processor =
        ProcessorPlan("peek-1", "source-1", [| "source-1" |], processor, failureAction, deadLetterFactory)

    let private sourcePlan sourceId =
        SourcePlan(
            sourceId,
            $"{sourceId}-topic",
            typeof<string>.FullName,
            RuntimeDeserializer(fun _ _ -> "order-1"),
            RuntimeValueDeserializer(fun _ _ -> RuntimeValueResult.Emit("created")))

    let private sinkPlan sinkId streamId sourceIds =
        SinkPlan(sinkId, streamId, sourceIds, skipSinkEncoder, RuntimeProcessorFailureAction.FailTopology)

    let private nodeIdText (RuntimeNodeId value) = value

    let private edgePair (edge: RuntimeEdge) =
        nodeIdText edge.FromNodeId, nodeIdText edge.ToNodeId

    let private assertEdge fromNodeId toNodeId (graph: RuntimeTopologyGraph) =
        Assert.Contains(graph.Edges, fun edge -> edgePair edge = (fromNodeId, toNodeId))

    let private sourceRecord () =
        SourceRecord("orders", 0, 1L, "order-1", "created", Array.empty<RuntimeHeader>, Nullable())

    [<Fact>]
    let ``runtime topology graph compiles source to processor`` () =
        let plan = TopologyPlan("default", [| sourcePlan "source-1" |], [| processorPlan noOpProcessor |], Array.empty<SinkPlan>)

        let graph = RuntimeTopologyGraph.compile plan

        Assert.Contains(graph.Nodes, fun node -> node.NodeId = RuntimeNodeId "source-1" && node.Kind = RuntimeNodeKind.SourceNode)
        Assert.Contains(graph.Nodes, fun node -> node.NodeId = RuntimeNodeId "peek-1" && node.Kind = RuntimeNodeKind.ProcessorNode)
        assertEdge "source-1" "peek-1" graph

    [<Fact>]
    let ``runtime topology graph compiles source to sink`` () =
        let plan = TopologyPlan("default", [| sourcePlan "source-1" |], Array.empty<ProcessorPlan>, [| sinkPlan "sink-1" "source-1" [| "source-1" |] |])

        let graph = RuntimeTopologyGraph.compile plan

        Assert.Contains(graph.Nodes, fun node -> node.NodeId = RuntimeNodeId "sink-1" && node.Kind = RuntimeNodeKind.SinkNode)
        assertEdge "source-1" "sink-1" graph

    [<Fact>]
    let ``runtime topology graph compiles source to processor to sink`` () =
        let plan = TopologyPlan("default", [| sourcePlan "source-1" |], [| processorPlan noOpProcessor |], [| sinkPlan "sink-1" "source-1" [| "source-1" |] |])

        let graph = RuntimeTopologyGraph.compile plan

        assertEdge "source-1" "peek-1" graph
        assertEdge "peek-1" "sink-1" graph

    [<Fact>]
    let ``runtime topology graph chains processors in declaration order`` () =
        let first = ProcessorPlan("peek-1", "source-1", [| "source-1" |], noOpProcessor, RuntimeProcessorFailureAction.FailTopology, deadLetterFactory)
        let second = ProcessorPlan("peek-2", "source-1", [| "source-1" |], noOpProcessor, RuntimeProcessorFailureAction.FailTopology, deadLetterFactory)
        let plan = TopologyPlan("default", [| sourcePlan "source-1" |], [| first; second |], [| sinkPlan "sink-1" "source-1" [| "source-1" |] |])

        let graph = RuntimeTopologyGraph.compile plan

        assertEdge "source-1" "peek-1" graph
        assertEdge "peek-1" "peek-2" graph
        assertEdge "peek-2" "sink-1" graph

    [<Fact>]
    let ``runtime topology graph compiles merged source streams`` () =
        let processor = ProcessorPlan("peek-1", "merge-1", [| "source-1"; "source-2" |], noOpProcessor, RuntimeProcessorFailureAction.FailTopology, deadLetterFactory)
        let plan = TopologyPlan("default", [| sourcePlan "source-1"; sourcePlan "source-2" |], [| processor |], Array.empty<SinkPlan>)

        let graph = RuntimeTopologyGraph.compile plan

        Assert.Contains(graph.Nodes, fun node -> node.NodeId = RuntimeNodeId "merge-1" && node.Kind = RuntimeNodeKind.MergeNode)
        assertEdge "source-1" "merge-1" graph
        assertEdge "source-2" "merge-1" graph
        assertEdge "merge-1" "peek-1" graph

    [<Fact>]
    let ``routing slip remains pending while work is pending`` () =
        let slip, _ = RoutingSlip.create "source-1" "orders" 0 1L |> RoutingSlip.addWork (RuntimeNodeId "peek-1") None

        Assert.Equal(RoutingSlipStatus.Pending, slip.Status)

    [<Fact>]
    let ``routing slip completes when all work is completed or dropped`` () =
        let slip, first = RoutingSlip.create "source-1" "orders" 0 1L |> RoutingSlip.addWork (RuntimeNodeId "peek-1") None
        let slip, second = slip |> RoutingSlip.addWork (RuntimeNodeId "sink-1") (Some first.WorkItemId)

        let completed = slip |> RoutingSlip.complete first.WorkItemId |> RoutingSlip.drop second.WorkItemId

        Assert.Equal(RoutingSlipStatus.Completed, completed.Status)

    [<Fact>]
    let ``routing slip keeps fan-out pending when one branch drops`` () =
        let slip, first = RoutingSlip.create "source-1" "orders" 0 1L |> RoutingSlip.addWork (RuntimeNodeId "peek-1") None
        let slip, second = slip |> RoutingSlip.addWork (RuntimeNodeId "peek-2") None

        let partiallyDropped = slip |> RoutingSlip.drop first.WorkItemId

        Assert.Equal(RoutingSlipStatus.Pending, partiallyDropped.Status)

        let completed = partiallyDropped |> RoutingSlip.complete second.WorkItemId

        Assert.Equal(RoutingSlipStatus.Completed, completed.Status)

    [<Fact>]
    let ``routing slip fails when any work item fails`` () =
        let slip, workItem = RoutingSlip.create "source-1" "orders" 0 1L |> RoutingSlip.addWork (RuntimeNodeId "peek-1") None

        let failed = slip |> RoutingSlip.fail workItem.WorkItemId (InvalidOperationException("boom"))

        Assert.Equal(RoutingSlipStatus.Failed, failed.Status)
        Assert.Equal(RoutingSlipWorkState.Failed, failed.WorkItems[0].State)

    [<Fact>]
    let ``routing slip pauses when any work item pauses`` () =
        let slip, workItem = RoutingSlip.create "source-1" "orders" 0 1L |> RoutingSlip.addWork (RuntimeNodeId "peek-1") None

        let paused = slip |> RoutingSlip.pause workItem.WorkItemId

        Assert.Equal(RoutingSlipStatus.Paused, paused.Status)
        Assert.Equal(RoutingSlipWorkState.Paused, paused.WorkItems[0].State)

    [<Fact>]
    let ``runtime value result can emit a value`` () =
        let result = RuntimeValueResult.Emit("created")

        Assert.True(result.ShouldEmit)
        Assert.Equal("created", result.Value :?> string)

    [<Fact>]
    let ``runtime value result can skip a value`` () =
        let result = RuntimeValueResult.Fail RuntimeValueFailureAction.Skip

        Assert.False(result.ShouldEmit)
        Assert.Equal(RuntimeValueFailureAction.Skip, result.FailureAction)

    [<Fact>]
    let ``runtime value result can pause a partition`` () =
        let result = RuntimeValueResult.Fail RuntimeValueFailureAction.PausePartition

        Assert.False(result.ShouldEmit)
        Assert.Equal(RuntimeValueFailureAction.PausePartition, result.FailureAction)

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
