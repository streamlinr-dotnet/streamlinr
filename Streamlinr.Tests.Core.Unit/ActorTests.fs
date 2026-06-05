namespace Streamlinr

module ActorTests =
    open System
    open System.Threading
    open System.Threading.Tasks
    open Xunit

    type private Message =
        | Ping of TaskCompletionSource<unit>
        | Switch
        | GetState of TaskCompletionSource<string>
        | Increment
        | Stop of TaskCompletionSource<unit>
        | Unexpected

    let private waitForCompletion (task: Task<'T>) =
        Assert.True(task.Wait(TimeSpan.FromSeconds 2.0), "Timed out waiting for actor response.")
        task.Result

    [<Fact>]
    let ``actor processes posted messages`` () =
        let processed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let actor =
            Actor.start (Behaviour (fun context ->
                match context.Message with
                | Ping completion ->
                    completion.SetResult()
                    Handled
                | _ -> Unhandled))

        Actor.post (Ping processed) actor

        waitForCompletion processed.Task

        Assert.True(processed.Task.IsCompletedSuccessfully)

    [<Fact>]
    let ``actor can change behaviour`` () =
        let state = TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)

        let secondBehaviour = Behaviour (fun context ->
            match context.Message with
            | GetState completion ->
                completion.SetResult "second"
                Handled
            | _ -> Unhandled)

        let actor =
            Actor.start (Behaviour (fun context ->
                match context.Message with
                | Switch -> Become secondBehaviour
                | GetState completion ->
                    completion.SetResult "first"
                    Handled
                | _ -> Unhandled))

        Actor.post Switch actor
        Actor.post (GetState state) actor

        Assert.Equal("second", waitForCompletion state.Task)

    [<Fact>]
    let ``actor terminates without processing later messages`` () =
        let count = ref 0
        let stopped = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let actor =
            Actor.start (Behaviour (fun context ->
                match context.Message with
                | Increment ->
                    Interlocked.Increment count |> ignore
                    Handled
                | Stop completion ->
                    completion.SetResult()
                    Terminate
                | _ -> Unhandled))

        Actor.post Increment actor
        Actor.post (Stop stopped) actor
        waitForCompletion stopped.Task
        Actor.post Increment actor

        Thread.Sleep 50

        Assert.Equal(1, count.Value)

    [<Fact>]
    let ``unhandled messages are reported`` () =
        let error = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)

        let actor =
            Actor.startWithErrorHandler error.SetResult (Behaviour (fun _ -> Unhandled))

        Actor.post Unexpected actor

        let exn = waitForCompletion error.Task
        Assert.IsType<InvalidOperationException>(exn) |> ignore
        Assert.Contains("unhandled", exn.Message)
