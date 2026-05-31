namespace Streamlinr

type private Actor<'TProtocol> = Actor of MailboxProcessor<'TProtocol>

type private Context<'TProtocol> =
    { Message: 'TProtocol
      Self: Actor<'TProtocol> }

and private Behaviour<'TProtocol> = Context<'TProtocol> -> ActorResult<'TProtocol>

and private ActorResult<'TProtocol> =
    | Ok
    | Become of Behaviour<'TProtocol>
    | Terminate
    | Unhandled

[<RequireQualifiedAccess>]
module private Agent =
    let startNew<'TProtocol> (initialBehaviour: Behaviour<'TProtocol>) =
        Actor (MailboxProcessor<'TProtocol>.Start(fun inbox ->
            let rec messageLoop currentBehaviour =
                async {
                    let! message = inbox.Receive()

                    match currentBehaviour { Message = message; Self = Actor inbox } with
                    | Ok -> return! messageLoop currentBehaviour
                    | Become newBehaviour -> return! messageLoop newBehaviour
                    | Terminate -> return ()
                    | Unhandled -> failwithf $"message of type '%A{message}' was unhandled"
                }

            Event.add (printfn "%A") inbox.Error

            messageLoop initialBehaviour))
