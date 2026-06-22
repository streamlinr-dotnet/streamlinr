namespace Streamlinr

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type internal RuntimeHeader(name: string, value: byte array) =
    member _.Name = name
    member _.Value = value

type internal RuntimeSerializationContext(topic: string, headers: IReadOnlyList<RuntimeHeader>) =
    member _.Topic = topic
    member _.Headers = headers

type internal RuntimeDeserializer = delegate of byte array * RuntimeSerializationContext -> obj

type internal RuntimeValueFailureAction =
    | Skip = 0
    | PausePartition = 1

type internal RuntimeValueResult private (value: obj, shouldEmit: bool, failureAction: RuntimeValueFailureAction) =
    member _.Value = value
    member _.ShouldEmit = shouldEmit
    member _.FailureAction = failureAction

    static member Emit(value: obj) =
        RuntimeValueResult(value, true, RuntimeValueFailureAction.Skip)

    static member Fail(action: RuntimeValueFailureAction) =
        RuntimeValueResult(null, false, action)

type internal RuntimeValueDeserializer = delegate of byte array * RuntimeSerializationContext -> RuntimeValueResult

type internal RuntimeSinkRecord(topic: string, key: byte array, value: byte array, headers: IReadOnlyList<RuntimeHeader>) =
    member _.Topic = topic
    member _.Key = key
    member _.Value = value
    member _.Headers = headers

type internal RuntimeSinkResult private (record: RuntimeSinkRecord, shouldProduce: bool) =
    member _.Record = record
    member _.ShouldProduce = shouldProduce

    static member Produce(record: RuntimeSinkRecord) =
        RuntimeSinkResult(record, true)

    static member Skip() =
        RuntimeSinkResult(null, false)

type internal SourceRecord(topic: string, partition: int, offset: int64, key: obj, value: obj, headers: IReadOnlyList<RuntimeHeader>, timestampUtc: Nullable<DateTimeOffset>) =
    member _.Topic = topic
    member _.Partition = partition
    member _.Offset = offset
    member _.Key = key
    member _.Value = value
    member _.Headers = headers
    member _.TimestampUtc = timestampUtc

type internal RuntimeSinkEncoder = delegate of SourceRecord -> RuntimeSinkResult

type internal SourceRecordBatch(batchId: Guid, sourceId: string, records: IReadOnlyList<SourceRecord>, receivedAtUtc: DateTimeOffset) =
    member _.BatchId = batchId
    member _.SourceId = sourceId
    member _.Records = records
    member _.ReceivedAtUtc = receivedAtUtc

type internal BatchProcessingResult(batchId: Guid, processedRecordCount: int) =
    member _.BatchId = batchId
    member _.ProcessedRecordCount = processedRecordCount

type internal RecordProcessor = delegate of SourceRecord * CancellationToken -> Task
type internal RuntimeDeadLetterFactory = delegate of SourceRecord * exn -> obj

type internal RuntimeProcessorFailureAction =
    | FailTopology = 0
    | Skip = 1
    | ContinueAsDeadLetter = 2
    | PausePartition = 3

type internal ProcessorPlan(processorId: string, streamId: string, sourceIds: IReadOnlyList<string>, processor: RecordProcessor, failureAction: RuntimeProcessorFailureAction, deadLetterFactory: RuntimeDeadLetterFactory) =
    member _.ProcessorId = processorId
    member _.StreamId = streamId
    member _.SourceIds = sourceIds
    member _.Processor = processor
    member _.FailureAction = failureAction
    member _.DeadLetterFactory = deadLetterFactory

type internal SinkPlan(sinkId: string, streamId: string, sourceIds: IReadOnlyList<string>, encoder: RuntimeSinkEncoder, failureAction: RuntimeProcessorFailureAction) =
    member _.SinkId = sinkId
    member _.StreamId = streamId
    member _.SourceIds = sourceIds
    member _.Encoder = encoder
    member _.FailureAction = failureAction

type internal SourcePlan(sourceId: string, topic: string, keyTypeName: string, keyDeserializer: RuntimeDeserializer, valueDeserializer: RuntimeValueDeserializer) =
    member _.SourceId = sourceId
    member _.Topic = topic
    member _.KeyTypeName = keyTypeName
    member _.KeyDeserializer = keyDeserializer
    member _.ValueDeserializer = valueDeserializer

type internal TopologyPlan(topologyName: string, sources: IReadOnlyList<SourcePlan>, processors: IReadOnlyList<ProcessorPlan>, sinks: IReadOnlyList<SinkPlan>) =
    member _.TopologyName = topologyName
    member _.Sources = sources
    member _.Processors = processors
    member _.Sinks = sinks

type internal RuntimeNodeId = RuntimeNodeId of string

type internal RuntimeNodeKind =
    | SourceNode = 0
    | MergeNode = 1
    | ProcessorNode = 2
    | SinkNode = 3

type internal RuntimeNode(nodeId: RuntimeNodeId, kind: RuntimeNodeKind) =
    member _.NodeId = nodeId
    member _.Kind = kind

type internal RuntimeEdge(fromNodeId: RuntimeNodeId, toNodeId: RuntimeNodeId) =
    member _.FromNodeId = fromNodeId
    member _.ToNodeId = toNodeId

type internal RuntimeTopologyGraph(nodes: IReadOnlyList<RuntimeNode>, edges: IReadOnlyList<RuntimeEdge>, sourceNodeIds: IReadOnlyList<RuntimeNodeId>, sinkNodeIds: IReadOnlyList<RuntimeNodeId>) =
    member _.Nodes = nodes
    member _.Edges = edges
    member _.SourceNodeIds = sourceNodeIds
    member _.SinkNodeIds = sinkNodeIds

[<RequireQualifiedAccess>]
type internal RoutingSlipWorkState =
    | Pending = 0
    | Completed = 1
    | Dropped = 2
    | Failed = 3
    | Paused = 4

[<RequireQualifiedAccess>]
type internal RoutingSlipStatus =
    | Pending = 0
    | Completed = 1
    | Failed = 2
    | Paused = 3

type internal RoutingSlipWorkItem =
    { WorkItemId: Guid
      NodeId: RuntimeNodeId
      ParentWorkItemId: Guid option
      State: RoutingSlipWorkState
      Failure: exn option }

type internal RoutingSlip =
    { SourceId: string
      Topic: string
      Partition: int
      Offset: int64
      WorkItems: IReadOnlyList<RoutingSlipWorkItem>
      Status: RoutingSlipStatus }

[<RequireQualifiedAccess>]
module internal RuntimeTopologyGraph =
    let private nodeId value = RuntimeNodeId value

    let private hasNode nodeId (nodes: ResizeArray<RuntimeNode>) =
        nodes |> Seq.exists (fun node -> node.NodeId = nodeId)

    let private addNode kind nodeId (nodes: ResizeArray<RuntimeNode>) =
        if not (hasNode nodeId nodes) then
            nodes.Add(RuntimeNode(nodeId, kind))

    let private addEdge fromNodeId toNodeId (edges: ResizeArray<RuntimeEdge>) =
        if edges |> Seq.exists (fun edge -> edge.FromNodeId = fromNodeId && edge.ToNodeId = toNodeId) |> not then
            edges.Add(RuntimeEdge(fromNodeId, toNodeId))

    let private nodeForStreamId sourceIds streamId (nodes: ResizeArray<RuntimeNode>) (edges: ResizeArray<RuntimeEdge>) =
        let streamNodeId = nodeId streamId

        if sourceIds |> Seq.contains streamId then
            streamNodeId
        elif hasNode streamNodeId nodes then
            streamNodeId
        else
            addNode RuntimeNodeKind.MergeNode streamNodeId nodes

            for sourceId in sourceIds do
                addEdge (nodeId sourceId) streamNodeId edges

            streamNodeId

    let compile (plan: TopologyPlan) =
        let nodes = ResizeArray<RuntimeNode>()
        let edges = ResizeArray<RuntimeEdge>()
        let sourceIds = plan.Sources |> Seq.map _.SourceId |> Seq.toArray
        let priorProcessors = ResizeArray<ProcessorPlan>()

        for source in plan.Sources do
            addNode RuntimeNodeKind.SourceNode (nodeId source.SourceId) nodes

        for processor in plan.Processors do
            let processorNodeId = nodeId processor.ProcessorId
            let upstreamNodeId =
                priorProcessors
                |> Seq.filter (fun prior -> prior.SourceIds |> Seq.exists (fun sourceId -> processor.SourceIds |> Seq.contains sourceId))
                |> Seq.tryLast
                |> Option.map (fun prior -> nodeId prior.ProcessorId)
                |> Option.defaultWith (fun () -> nodeForStreamId sourceIds processor.StreamId nodes edges)

            addNode RuntimeNodeKind.ProcessorNode processorNodeId nodes
            addEdge upstreamNodeId processorNodeId edges
            priorProcessors.Add processor

        for sink in plan.Sinks do
            let sinkNodeId = nodeId sink.SinkId
            let upstreamNodeId =
                plan.Processors
                |> Seq.filter (fun processor -> processor.SourceIds |> Seq.exists (fun sourceId -> sink.SourceIds |> Seq.contains sourceId))
                |> Seq.tryLast
                |> Option.map (fun processor -> nodeId processor.ProcessorId)
                |> Option.defaultWith (fun () -> nodeForStreamId sourceIds sink.StreamId nodes edges)

            addNode RuntimeNodeKind.SinkNode sinkNodeId nodes
            addEdge upstreamNodeId sinkNodeId edges

        RuntimeTopologyGraph(
            nodes |> Seq.toArray,
            edges |> Seq.toArray,
            sourceIds |> Array.map nodeId,
            plan.Sinks |> Seq.map (fun sink -> nodeId sink.SinkId) |> Seq.toArray)

    let tryFindNode nodeId (graph: RuntimeTopologyGraph) =
        graph.Nodes |> Seq.tryFind (fun node -> node.NodeId = nodeId)

    let outgoing nodeId (graph: RuntimeTopologyGraph) =
        graph.Edges
        |> Seq.filter (fun edge -> edge.FromNodeId = nodeId)
        |> Seq.map _.ToNodeId
        |> Seq.toArray

[<RequireQualifiedAccess>]
module internal RoutingSlip =
    let private classify workItems =
        if workItems |> Seq.exists (fun item -> item.State = RoutingSlipWorkState.Failed) then
            RoutingSlipStatus.Failed
        elif workItems |> Seq.exists (fun item -> item.State = RoutingSlipWorkState.Paused) then
            RoutingSlipStatus.Paused
        elif workItems |> Seq.exists (fun item -> item.State = RoutingSlipWorkState.Pending) then
            RoutingSlipStatus.Pending
        else
            RoutingSlipStatus.Completed

    let create sourceId topic partition offset =
        { SourceId = sourceId
          Topic = topic
          Partition = partition
          Offset = offset
          WorkItems = Array.empty<RoutingSlipWorkItem>
          Status = RoutingSlipStatus.Completed }

    let addWork nodeId parentWorkItemId slip =
        let workItem =
            { WorkItemId = Guid.NewGuid()
              NodeId = nodeId
              ParentWorkItemId = parentWorkItemId
              State = RoutingSlipWorkState.Pending
              Failure = None }

        let workItems =
            slip.WorkItems
            |> Seq.append [| workItem |]
            |> Seq.toArray

        { slip with WorkItems = workItems; Status = classify workItems }, workItem

    let private mark state failure workItemId slip =
        let workItems =
            slip.WorkItems
            |> Seq.map (fun item ->
                if item.WorkItemId = workItemId then
                    { item with State = state; Failure = failure }
                else
                    item)
            |> Seq.toArray

        { slip with WorkItems = workItems; Status = classify workItems }

    let complete workItemId slip = mark RoutingSlipWorkState.Completed None workItemId slip
    let drop workItemId slip = mark RoutingSlipWorkState.Dropped None workItemId slip
    let fail workItemId error slip = mark RoutingSlipWorkState.Failed (Some error) workItemId slip
    let pause workItemId slip = mark RoutingSlipWorkState.Paused None workItemId slip

    let tryPending slip =
        slip.WorkItems |> Seq.tryFind (fun item -> item.State = RoutingSlipWorkState.Pending)

type internal RuntimePlan(applicationId: string, bootstrapServers: string, topologies: IReadOnlyList<TopologyPlan>) =
    member _.ApplicationId = applicationId
    member _.BootstrapServers = bootstrapServers
    member _.Topologies = topologies

type internal ConsumerPlan(applicationId: string, bootstrapServers: string, topic: string, groupId: string, sourceId: string, maxBatchSize: int, pollTimeout: TimeSpan) =
    member _.ApplicationId = applicationId
    member _.BootstrapServers = bootstrapServers
    member _.Topic = topic
    member _.GroupId = groupId
    member _.SourceId = sourceId
    member _.MaxBatchSize = maxBatchSize
    member _.PollTimeout = pollTimeout

type internal RuntimeStatus =
    | Created
    | Starting
    | Running
    | Stopping
    | Stopped
    | Fatal of string

type internal ProcessorError =
    { ProcessorId: string
      BatchId: Guid
      SourceId: string
      Record: SourceRecord
      FailureAction: RuntimeProcessorFailureAction
      DeadLetterFactory: RuntimeDeadLetterFactory
      Error: exn }

type internal SinkError =
    { SinkId: string
      BatchId: Guid
      SourceId: string
      Record: SourceRecord
      FailureAction: RuntimeProcessorFailureAction
      Error: exn }

type internal RuntimeStartError =
    { Message: string }

type internal TopologyStartError =
    { Message: string }

type internal ConsumerStartError =
    { Message: string }

type internal ProcessorMessage =
    | ProcessBatch of SourceRecordBatch * AsyncReplyChannel<Result<BatchProcessingResult, ProcessorError>>
    | StopProcessor of AsyncReplyChannel<unit>

type internal SinkMessage =
    | WriteBatch of SourceRecordBatch * AsyncReplyChannel<Result<BatchProcessingResult, SinkError>>
    | StopSink of AsyncReplyChannel<unit>

type internal TopologyStatus =
    | TopologyCreated
    | TopologyRunning
    | TopologyStopping
    | TopologyStopped
    | TopologyFatal of string

type internal CompletedSourceOffset(topic: string, partition: int, offset: int64) =
    member _.Topic = topic
    member _.Partition = partition
    member _.Offset = offset

type internal TopologyBatchResult =
    | BatchCompleted of IReadOnlyList<CompletedSourceOffset>
    | BatchPaused
    | BatchFailed

type internal TopologyMessage =
    | StartTopology of TopologyPlan * AsyncReplyChannel<Result<unit, TopologyStartError>>
    | SourceBatchReceived of SourceRecordBatch * AsyncReplyChannel<TopologyBatchResult>
    | SourceFailed of topic: string * error: exn
    | ProcessorFailed of processorId: string * error: exn
    | SinkFailed of sinkId: string * error: exn
    | StopTopology of AsyncReplyChannel<unit>
    | GetTopologyStatus of AsyncReplyChannel<TopologyStatus>

type internal KafkaConsumerMessage =
    | StartConsuming of ConsumerPlan * SourcePlan * target: Actor<TopologyMessage> * AsyncReplyChannel<Result<unit, ConsumerStartError>>
    | Poll
    | PausePartition of partition: int * AsyncReplyChannel<unit>
    | StopConsuming of AsyncReplyChannel<unit>

type internal RuntimeSupervisorMessage =
    | Start of RuntimePlan * AsyncReplyChannel<Result<unit, RuntimeStartError>>
    | Stop of AsyncReplyChannel<unit>
    | ChildFailed of role: string * error: exn
    | GetStatus of AsyncReplyChannel<RuntimeStatus>

[<RequireQualifiedAccess>]
module internal ProcessorActor =
    let start (processorPlan: ProcessorPlan) (cancellationToken: CancellationToken) =
        let running = Behaviour (fun (context: ActorContext<ProcessorMessage>) ->
            match context.Message with
            | ProcessBatch (batch, reply) ->
                task {
                    try
                        for record in batch.Records do
                            try
                                do! processorPlan.Processor.Invoke(record, cancellationToken)
                            with error ->
                                return reply.Reply(Error {
                                    ProcessorId = processorPlan.ProcessorId
                                    BatchId = batch.BatchId
                                    SourceId = batch.SourceId
                                    Record = record
                                    FailureAction = processorPlan.FailureAction
                                    DeadLetterFactory = processorPlan.DeadLetterFactory
                                    Error = error
                                })

                        reply.Reply(Ok(BatchProcessingResult(batch.BatchId, batch.Records.Count)))
                    with error ->
                        let record = batch.Records[0]
                        reply.Reply(Error {
                            ProcessorId = processorPlan.ProcessorId
                            BatchId = batch.BatchId
                            SourceId = batch.SourceId
                            Record = record
                            FailureAction = processorPlan.FailureAction
                            DeadLetterFactory = processorPlan.DeadLetterFactory
                            Error = error
                        })
                }
                |> ignore

                Handled
            | StopProcessor reply ->
                reply.Reply()
                Terminate)

        Actor.start running

[<RequireQualifiedAccess>]
module internal SinkActor =
    let private buildProducer (bootstrapServers: string) =
        let config =
            Confluent.Kafka.ProducerConfig(
                BootstrapServers = bootstrapServers,
                MessageTimeoutMs = 10_000)

        Confluent.Kafka.ProducerBuilder<byte array, byte array>(config).Build()

    let private toKafkaHeaders (headers: IReadOnlyList<RuntimeHeader>) =
        let kafkaHeaders = Confluent.Kafka.Headers()

        for header in headers do
            kafkaHeaders.Add(header.Name, header.Value)

        kafkaHeaders

    let start (bootstrapServers: string) (sinkPlan: SinkPlan) =
        let producer = buildProducer bootstrapServers

        let running = Behaviour (fun (context: ActorContext<SinkMessage>) ->
            match context.Message with
            | WriteBatch (batch, reply) ->
                task {
                    try
                        let mutable producedRecordCount = 0

                        for record in batch.Records do
                            try
                                let sinkResult = sinkPlan.Encoder.Invoke record

                                if sinkResult.ShouldProduce then
                                    let sinkRecord = sinkResult.Record
                                    let message =
                                        Confluent.Kafka.Message<byte array, byte array>(
                                            Key = sinkRecord.Key,
                                            Value = sinkRecord.Value,
                                            Headers = toKafkaHeaders sinkRecord.Headers)

                                    let! _ = producer.ProduceAsync(sinkRecord.Topic, message)
                                    producedRecordCount <- producedRecordCount + 1
                                    ()
                            with error ->
                                return reply.Reply(Error {
                                    SinkId = sinkPlan.SinkId
                                    BatchId = batch.BatchId
                                    SourceId = batch.SourceId
                                    Record = record
                                    FailureAction = sinkPlan.FailureAction
                                    Error = error
                                })

                        reply.Reply(Ok(BatchProcessingResult(batch.BatchId, producedRecordCount)))
                    with error ->
                        let record = batch.Records[0]
                        reply.Reply(Error {
                            SinkId = sinkPlan.SinkId
                            BatchId = batch.BatchId
                            SourceId = batch.SourceId
                            Record = record
                            FailureAction = sinkPlan.FailureAction
                            Error = error
                        })
                }
                |> ignore

                Handled
            | StopSink reply ->
                producer.Flush(TimeSpan.FromSeconds 10.0) |> ignore
                producer.Dispose()
                reply.Reply()
                Terminate)

        Actor.start running

[<RequireQualifiedAccess>]
module internal KafkaConsumerActor =
    let private toTimestampUtc (timestamp: Confluent.Kafka.Timestamp) =
        if timestamp.Type = Confluent.Kafka.TimestampType.NotAvailable then
            Nullable<DateTimeOffset>()
        else
            Nullable(DateTimeOffset(timestamp.UtcDateTime))

    let private toRuntimeHeaders (headers: Confluent.Kafka.Headers) =
        if isNull (box headers) then
            Array.empty<RuntimeHeader> :> IReadOnlyList<RuntimeHeader>
        else
            headers
            |> Seq.map (fun header -> RuntimeHeader(header.Key, header.GetValueBytes()))
            |> Seq.toArray
            :> IReadOnlyList<RuntimeHeader>

    let private toSourceRecord (source: SourcePlan) (result: Confluent.Kafka.ConsumeResult<byte array, byte array>) =
        let headers = toRuntimeHeaders result.Message.Headers
        let context = RuntimeSerializationContext(result.Topic, headers)
        let valueResult = source.ValueDeserializer.Invoke(result.Message.Value, context)

        if valueResult.ShouldEmit then
            Ok(Some(SourceRecord(
                result.Topic,
                result.Partition.Value,
                result.Offset.Value,
                source.KeyDeserializer.Invoke(result.Message.Key, context),
                valueResult.Value,
                headers,
                toTimestampUtc result.Message.Timestamp)))
        else
            match valueResult.FailureAction with
            | RuntimeValueFailureAction.Skip -> Ok None
            | RuntimeValueFailureAction.PausePartition -> Error(result.Partition.Value)
            | _ -> Ok None

    let private buildConsumer (plan: ConsumerPlan) =
        let config =
            Confluent.Kafka.ConsumerConfig(
                BootstrapServers = plan.BootstrapServers,
                GroupId = plan.GroupId,
                AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
                EnableAutoCommit = false)

        Confluent.Kafka.ConsumerBuilder<byte array, byte array>(config).Build()

    let private commitResult (consumer: Confluent.Kafka.IConsumer<byte array, byte array>) (result: Confluent.Kafka.ConsumeResult<byte array, byte array>) =
        consumer.Commit result

    let private commitCompletedOffsets (consumer: Confluent.Kafka.IConsumer<byte array, byte array>) (offsets: IReadOnlyList<CompletedSourceOffset>) =
        if offsets.Count > 0 then
            offsets
            |> Seq.map (fun offset ->
                Confluent.Kafka.TopicPartitionOffset(
                    offset.Topic,
                    Confluent.Kafka.Partition(offset.Partition),
                    Confluent.Kafka.Offset(offset.Offset + 1L)))
            |> Seq.toArray
            |> consumer.Commit
            |> ignore

    let private tryCollectBatch (consumer: Confluent.Kafka.IConsumer<byte array, byte array>) (plan: ConsumerPlan) (source: SourcePlan) =
        try
            let records = ResizeArray<SourceRecord>()
            let mutable keepPolling = true

            while keepPolling && records.Count < plan.MaxBatchSize do
                let result = consumer.Consume(plan.PollTimeout)

                if isNull (box result) then
                    keepPolling <- false
                elif result.IsPartitionEOF then
                    keepPolling <- false
                else
                    match toSourceRecord source result with
                    | Ok (Some record) -> records.Add record
                    | Ok None -> commitResult consumer result
                    | Error partition -> consumer.Pause([| Confluent.Kafka.TopicPartition(plan.Topic, Confluent.Kafka.Partition(partition)) |])

                    if plan.MaxBatchSize = 1 then
                        keepPolling <- false

            if records.Count = 0 then
                Ok None
            else
                Ok(Some(SourceRecordBatch(Guid.NewGuid(), plan.SourceId, records, DateTimeOffset.UtcNow)))
        with error ->
            Error error

    let start () =
        let rec notStarted = Behaviour(fun (context: ActorContext<KafkaConsumerMessage>) ->
            match context.Message with
            | StartConsuming (plan, source, target, reply) ->
                try
                    let consumer = buildConsumer plan
                    consumer.Subscribe plan.Topic
                    reply.Reply(Ok())
                    Actor.post Poll context.Self
                    Become(running plan source target consumer)
                with error ->
                    reply.Reply(Error { Message = error.Message })
                    Terminate
            | StopConsuming reply ->
                reply.Reply()
                Terminate
            | PausePartition (_, reply) ->
                reply.Reply()
                Handled
            | _ -> Unhandled)

        and running plan source target consumer = Behaviour (fun (context: ActorContext<KafkaConsumerMessage>) ->
            match context.Message with
            | Poll ->
                match tryCollectBatch consumer plan source with
                | Ok (Some batch) ->
                    task {
                        let! result = Actor.postAndAsyncReply (fun channel -> SourceBatchReceived(batch, channel)) target |> Async.StartAsTask

                        match result with
                        | BatchCompleted offsets -> commitCompletedOffsets consumer offsets
                        | BatchPaused
                        | BatchFailed -> ()

                        Actor.post Poll context.Self
                    }
                    |> ignore

                    Handled
                | Ok None ->
                    Actor.post Poll context.Self
                    Handled
                | Error error ->
                    Actor.post (SourceFailed(plan.Topic, error)) target
                    Terminate
            | PausePartition (partition, reply) ->
                consumer.Pause([| Confluent.Kafka.TopicPartition(plan.Topic, Confluent.Kafka.Partition(partition)) |])
                reply.Reply()
                Handled
            | StopConsuming reply ->
                consumer.Close()
                consumer.Dispose()
                reply.Reply()
                Terminate
            | _ -> Unhandled)

        Actor.start notStarted

[<RequireQualifiedAccess>]
module internal TopologyActor =
    let private validate (plan: TopologyPlan) : Result<unit, TopologyStartError> =
        if plan.Sources.Count = 0 then
            Error({ Message = "At least one source is required." } : TopologyStartError)
        elif plan.Processors.Count = 0 && plan.Sinks.Count = 0 then
            Error({ Message = "At least one processor or sink is required." } : TopologyStartError)
        else
            Ok()

    let private stopConsumers consumers =
        for _, consumer in consumers do
            Actor.postAndAsyncReply StopConsuming consumer |> Async.RunSynchronously

    let private stopProcessors processors =
        for _, processor in processors do
            Actor.postAndAsyncReply StopProcessor processor |> Async.RunSynchronously

    let private stopSinks sinks =
        for _, sink in sinks do
            Actor.postAndAsyncReply StopSink sink |> Async.RunSynchronously

    let private pauseConsumerPartition sourceId partition consumers =
        consumers
        |> Seq.tryFind (fun (consumerSourceId, _) -> consumerSourceId = sourceId)
        |> Option.iter (fun (_, consumer) -> Actor.postAndAsyncReply (fun channel -> PausePartition(partition, channel)) consumer |> Async.RunSynchronously)

    let private singleRecordBatch (sourceId: string) (record: SourceRecord) =
        SourceRecordBatch(Guid.NewGuid(), sourceId, [| record |], DateTimeOffset.UtcNow)

    let private deadLetterRecord (record: SourceRecord) (deadLetterValue: obj) =
        SourceRecord(
            record.Topic,
            record.Partition,
            record.Offset,
            record.Key,
            deadLetterValue,
            record.Headers,
            record.TimestampUtc)

    let private scheduleOutgoing graph (parentWorkItem: RoutingSlipWorkItem) record (workItemRecords: Dictionary<Guid, SourceRecord>) slip =
        let mutable currentSlip = slip

        for childNodeId in RuntimeTopologyGraph.outgoing parentWorkItem.NodeId graph do
            let updatedSlip, childWorkItem = RoutingSlip.addWork childNodeId (Some parentWorkItem.WorkItemId) currentSlip
            workItemRecords[childWorkItem.WorkItemId] <- record
            currentSlip <- updatedSlip

        currentSlip

    let start (runtimePlan: RuntimePlan) (supervisor: Actor<RuntimeSupervisorMessage>) (cancellationToken: CancellationToken) =
        let rec created = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | StartTopology (plan, reply) ->
                match validate plan with
                | Error error ->
                    reply.Reply(Error error)
                    Terminate
                | Ok () ->
                    let graph = RuntimeTopologyGraph.compile plan

                    let processors =
                        plan.Processors
                        |> Seq.map (fun processor -> processor, ProcessorActor.start processor cancellationToken)
                        |> Seq.toList

                    let sinks =
                        plan.Sinks
                        |> Seq.map (fun sink -> sink, SinkActor.start runtimePlan.BootstrapServers sink)
                        |> Seq.toList

                    let consumers = ResizeArray<string * Actor<KafkaConsumerMessage>>()
                    let mutable startError = None

                    for source in plan.Sources do
                        if startError.IsNone then
                            let consumer = KafkaConsumerActor.start ()
                            let consumerPlan = ConsumerPlan(runtimePlan.ApplicationId, runtimePlan.BootstrapServers, source.Topic, runtimePlan.ApplicationId, source.SourceId, 1, TimeSpan.FromMilliseconds 100.0)
                            let started = Actor.postAndAsyncReply (fun channel -> StartConsuming(consumerPlan, source, context.Self, channel)) consumer |> Async.RunSynchronously

                            match started with
                            | Ok () -> consumers.Add((source.SourceId, consumer))
                            | Error error -> startError <- Some error.Message

                    match startError with
                    | None ->
                        reply.Reply(Ok())
                        Become(running graph processors sinks (consumers |> Seq.toList))
                    | Some error ->
                        stopConsumers consumers
                        stopProcessors processors
                        stopSinks sinks
                        reply.Reply(Error { Message = error })
                        Terminate
            | GetTopologyStatus reply ->
                reply.Reply TopologyCreated
                Handled
            | StopTopology reply ->
                reply.Reply()
                Terminate
            | _ -> Unhandled)

        and running graph processors sinks consumers = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | SourceBatchReceived (batch, reply) ->
                task {
                    let completedOffsets = ResizeArray<CompletedSourceOffset>()
                    let mutable terminalResult = None

                    for record in batch.Records do
                        if terminalResult.IsNone then
                            let sourceNodeId = RuntimeNodeId batch.SourceId
                            let workItemRecords = Dictionary<Guid, SourceRecord>()
                            let mutable slip = RoutingSlip.create batch.SourceId record.Topic record.Partition record.Offset

                            for childNodeId in RuntimeTopologyGraph.outgoing sourceNodeId graph do
                                let updatedSlip, workItem = RoutingSlip.addWork childNodeId None slip
                                workItemRecords[workItem.WorkItemId] <- record
                                slip <- updatedSlip

                            let mutable keepProcessing = true

                            while keepProcessing do
                                match RoutingSlip.tryPending slip with
                                | None -> keepProcessing <- false
                                | Some workItem when slip.Status <> RoutingSlipStatus.Pending -> keepProcessing <- false
                                | Some workItem ->
                                    let routedRecord = workItemRecords[workItem.WorkItemId]

                                    match RuntimeTopologyGraph.tryFindNode workItem.NodeId graph with
                                    | None ->
                                        slip <- RoutingSlip.fail workItem.WorkItemId (InvalidOperationException("Routing slip referenced an unknown runtime node.")) slip
                                    | Some node ->
                                        match node.Kind with
                                        | RuntimeNodeKind.SourceNode ->
                                            slip <- RoutingSlip.complete workItem.WorkItemId slip
                                            slip <- scheduleOutgoing graph workItem routedRecord workItemRecords slip
                                        | RuntimeNodeKind.MergeNode ->
                                            slip <- RoutingSlip.complete workItem.WorkItemId slip
                                            slip <- scheduleOutgoing graph workItem routedRecord workItemRecords slip
                                        | RuntimeNodeKind.ProcessorNode ->
                                            match processors |> Seq.tryFind (fun (processorPlan, _) -> RuntimeNodeId processorPlan.ProcessorId = workItem.NodeId) with
                                            | None ->
                                                slip <- RoutingSlip.fail workItem.WorkItemId (InvalidOperationException("Routing slip referenced an unknown processor node.")) slip
                                            | Some (processorPlan, processor) ->
                                                let! result = Actor.postAndAsyncReply (fun channel -> ProcessBatch(singleRecordBatch batch.SourceId routedRecord, channel)) processor |> Async.StartAsTask

                                                match result with
                                                | Ok _ ->
                                                    slip <- RoutingSlip.complete workItem.WorkItemId slip
                                                    slip <- scheduleOutgoing graph workItem routedRecord workItemRecords slip
                                                | Error error ->
                                                    match error.FailureAction with
                                                    | RuntimeProcessorFailureAction.FailTopology ->
                                                        slip <- RoutingSlip.fail workItem.WorkItemId error.Error slip
                                                        Actor.post (ProcessorFailed(processorPlan.ProcessorId, error.Error)) context.Self
                                                    | RuntimeProcessorFailureAction.Skip ->
                                                        slip <- RoutingSlip.drop workItem.WorkItemId slip
                                                    | RuntimeProcessorFailureAction.ContinueAsDeadLetter ->
                                                        let deadLetter = deadLetterRecord error.Record (error.DeadLetterFactory.Invoke(error.Record, error.Error))
                                                        slip <- RoutingSlip.complete workItem.WorkItemId slip
                                                        slip <- scheduleOutgoing graph workItem deadLetter workItemRecords slip
                                                    | RuntimeProcessorFailureAction.PausePartition ->
                                                        slip <- RoutingSlip.pause workItem.WorkItemId slip
                                                        pauseConsumerPartition error.SourceId error.Record.Partition consumers
                                                    | _ ->
                                                        slip <- RoutingSlip.fail workItem.WorkItemId error.Error slip
                                                        Actor.post (ProcessorFailed(processorPlan.ProcessorId, error.Error)) context.Self
                                        | RuntimeNodeKind.SinkNode ->
                                            match sinks |> Seq.tryFind (fun (sinkPlan, _) -> RuntimeNodeId sinkPlan.SinkId = workItem.NodeId) with
                                            | None ->
                                                slip <- RoutingSlip.fail workItem.WorkItemId (InvalidOperationException("Routing slip referenced an unknown sink node.")) slip
                                            | Some (sinkPlan, sink) ->
                                                let! result = Actor.postAndAsyncReply (fun channel -> WriteBatch(singleRecordBatch batch.SourceId routedRecord, channel)) sink |> Async.StartAsTask

                                                match result with
                                                | Ok sinkResult when sinkResult.ProcessedRecordCount = 0 -> slip <- RoutingSlip.drop workItem.WorkItemId slip
                                                | Ok _ -> slip <- RoutingSlip.complete workItem.WorkItemId slip
                                                | Error error ->
                                                    match error.FailureAction with
                                                    | RuntimeProcessorFailureAction.FailTopology ->
                                                        slip <- RoutingSlip.fail workItem.WorkItemId error.Error slip
                                                        Actor.post (SinkFailed(sinkPlan.SinkId, error.Error)) context.Self
                                                    | RuntimeProcessorFailureAction.Skip
                                                    | RuntimeProcessorFailureAction.ContinueAsDeadLetter ->
                                                        slip <- RoutingSlip.drop workItem.WorkItemId slip
                                                    | RuntimeProcessorFailureAction.PausePartition ->
                                                        slip <- RoutingSlip.pause workItem.WorkItemId slip
                                                        pauseConsumerPartition error.SourceId error.Record.Partition consumers
                                                    | _ ->
                                                        slip <- RoutingSlip.fail workItem.WorkItemId error.Error slip
                                                        Actor.post (SinkFailed(sinkPlan.SinkId, error.Error)) context.Self
                                        | _ ->
                                            slip <- RoutingSlip.fail workItem.WorkItemId (InvalidOperationException("Routing slip referenced an unsupported runtime node kind.")) slip

                            match slip.Status with
                            | RoutingSlipStatus.Completed -> completedOffsets.Add(CompletedSourceOffset(record.Topic, record.Partition, record.Offset))
                            | RoutingSlipStatus.Paused -> terminalResult <- Some BatchPaused
                            | RoutingSlipStatus.Failed -> terminalResult <- Some BatchFailed
                            | RoutingSlipStatus.Pending -> terminalResult <- Some BatchFailed
                            | _ -> terminalResult <- Some BatchFailed

                    match terminalResult with
                    | Some result -> reply.Reply result
                    | None -> reply.Reply(BatchCompleted(completedOffsets |> Seq.toArray))
                }
                |> ignore

                Handled
            | SourceFailed (topic, error) ->
                stopConsumers consumers
                stopProcessors processors
                stopSinks sinks
                Actor.post (ChildFailed($"source:{topic}", error)) supervisor
                Become(failed error.Message)
            | ProcessorFailed (processorId, error) ->
                stopConsumers consumers
                stopProcessors processors
                stopSinks sinks
                Actor.post (ChildFailed($"processor:{processorId}", error)) supervisor
                Become(failed error.Message)
            | SinkFailed (sinkId, error) ->
                stopConsumers consumers
                stopProcessors processors
                stopSinks sinks
                Actor.post (ChildFailed($"sink:{sinkId}", error)) supervisor
                Become(failed error.Message)
            | StopTopology reply ->
                stopConsumers consumers
                stopProcessors processors
                stopSinks sinks

                reply.Reply()
                Terminate
            | GetTopologyStatus reply ->
                reply.Reply TopologyRunning
                Handled
            | _ -> Unhandled)

        and failed message = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | StopTopology reply ->
                reply.Reply()
                Terminate
            | GetTopologyStatus reply ->
                reply.Reply(TopologyFatal message)
                Handled
            | _ -> Handled)

        Actor.start created

[<RequireQualifiedAccess>]
module internal RuntimeSupervisor =
    let start (completion: TaskCompletionSource<exn option>) (cancellationToken: CancellationToken) =
        let rec created = Behaviour(fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | Start (plan, reply) ->
                if String.IsNullOrWhiteSpace plan.ApplicationId then
                    reply.Reply(Error { Message = "ApplicationId is required." })
                    Handled
                elif String.IsNullOrWhiteSpace plan.BootstrapServers then
                    reply.Reply(Error { Message = "BootstrapServers is required." })
                    Handled
                elif plan.Topologies.Count <> 1 then
                    reply.Reply(Error { Message = "Exactly one topology is supported by the initial runtime." })
                    Handled
                else
                    let topologyActor = TopologyActor.start plan context.Self cancellationToken
                    let started = Actor.postAndAsyncReply (fun channel -> StartTopology(plan.Topologies[0], channel)) topologyActor |> Async.RunSynchronously

                    match started with
                    | Ok () ->
                        reply.Reply(Ok())
                        Become(running topologyActor)
                    | Error error ->
                        reply.Reply(Error { Message = error.Message })
                        Handled
            | GetStatus reply ->
                reply.Reply Created
                Handled
            | Stop reply ->
                reply.Reply()
                completion.TrySetResult None |> ignore
                Terminate
            | _ -> Unhandled)

        and running topologyActor = Behaviour (fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | ChildFailed (_, error) ->
                completion.TrySetResult(Some error) |> ignore
                Become(fatal error.Message topologyActor)
            | GetStatus reply ->
                reply.Reply Running
                Handled
            | Stop reply ->
                Actor.postAndAsyncReply StopTopology topologyActor |> Async.RunSynchronously
                reply.Reply()
                completion.TrySetResult None |> ignore
                Terminate
            | _ -> Unhandled)

        and fatal message topologyActor = Behaviour (fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | GetStatus reply ->
                reply.Reply(Fatal message)
                Handled
            | Stop reply ->
                Actor.postAndAsyncReply StopTopology topologyActor |> Async.RunSynchronously
                reply.Reply()
                Terminate
            | _ -> Handled)

        Actor.start created

type internal Runtime =
    static member RunAsync(plan: RuntimePlan, cancellationToken: CancellationToken) : Task =
        task {
            let completion = TaskCompletionSource<exn option>(TaskCreationOptions.RunContinuationsAsynchronously)
            let supervisor = RuntimeSupervisor.start completion cancellationToken
            let! started = Actor.postAndAsyncReply (fun reply -> Start(plan, reply)) supervisor |> Async.StartAsTask

            match started with
            | Error error -> invalidOp error.Message
            | Ok () -> ()

            let cancellation = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
            let! completed = Task.WhenAny(completion.Task :> Task, cancellation)

            if Object.ReferenceEquals(completed, cancellation) then
                let! _ = Actor.postAndAsyncReply Stop supervisor |> Async.StartAsTask
                ()
            else
                let! completionResult = completion.Task

                match completionResult with
                | None -> ()
                | Some error -> raise error
        }
