namespace Streamlinr

open System

type internal Actor<'TProtocol> = private Actor of MailboxProcessor<'TProtocol>

type internal ActorContext<'TProtocol> =
    { Message: 'TProtocol
      Self: Actor<'TProtocol> }

and internal Behaviour<'TProtocol> = ActorContext<'TProtocol> -> ActorResult<'TProtocol>

and internal ActorResult<'TProtocol> =
    | Handled
    | Become of Behaviour<'TProtocol>
    | Terminate
    | Unhandled

[<RequireQualifiedAccess>]
module internal Actor =
    let startWithErrorHandler<'TProtocol> (onError: exn -> unit) (initialBehaviour: Behaviour<'TProtocol>) =
        let mailbox =
            MailboxProcessor<'TProtocol>.Start(fun inbox ->
                let rec messageLoop currentBehaviour =
                    async {
                        let! message = inbox.Receive()

                        match currentBehaviour { Message = message; Self = Actor inbox } with
                        | Handled -> return! messageLoop currentBehaviour
                        | Become newBehaviour -> return! messageLoop newBehaviour
                        | Terminate -> return ()
                        | Unhandled -> raise (InvalidOperationException $"message '%A{message}' was unhandled")
                    }

                inbox.Error.Add onError

                messageLoop initialBehaviour)

        Actor mailbox

    let start<'TProtocol> (initialBehaviour: Behaviour<'TProtocol>) =
        startWithErrorHandler ignore initialBehaviour

    let post<'TProtocol> message (Actor mailbox: Actor<'TProtocol>) =
        mailbox.Post message
