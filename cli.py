import asyncio
import json
import os
import queue
import sys
import threading
from datetime import datetime

import RNS
import database
from core import MeshChatCore, _validate_display_name, parse_lxmf_display_name

PROMPT = "> "

RNS.loglevel = RNS.LOG_WARNING


def run_asyncio_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def submit_to_loop(loop, coroutine):
    future = asyncio.run_coroutine_threadsafe(coroutine, loop)

    def _report_result(done_future):
        try:
            done_future.result()
        except Exception as error:
            sys.stdout.write("\r\033[K") # Очистка текущей строки терминала
            print("error: " + str(error))
            print(PROMPT, end="", flush=True)

    future.add_done_callback(_report_result)
    return future


def format_timestamp(timestamp):
    if not timestamp:
        return "?"
    try:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "?"


def attachment_names_from_fields(fields_text):
    try:
        fields = json.loads(fields_text) if fields_text else {}
    except Exception:
        fields = {}
    if not isinstance(fields, dict):
        return []
    try:
        raw_attachments = fields.get("file_attachments") or []
    except Exception:
        return []
    if not isinstance(raw_attachments, (list, tuple)):
        return []
    names = []
    for attachment in raw_attachments[:32]:
        try:
            if isinstance(attachment, dict):
                name = str(attachment.get("file_name", "?"))
            else:
                name = "?"
            if len(name) > 255:
                name = name[:255]
            names.append(name)
        except Exception:
            names.append("?")
    return names


def render_event(core, event_type, payload, attachment_dir):
    lines = []
    if event_type == "message":
        lines.append("incoming message from " + str(payload.get("source_hash", "?"))[:64] +
                     " (" + format_timestamp(payload.get("timestamp")) + ")")
        lines.append("    " + str(payload.get("content", ""))[:2000])
        try:
            fields = payload.get("fields", {})
            attachments = fields.get("file_attachments") if isinstance(fields, dict) else None
            attachments = attachments or []
        except Exception:
            attachments = []
        if not isinstance(attachments, (list, tuple)):
            attachments = []
        if attachments:
            names = []
            for attachment in attachments[:32]:
                try:
                    if isinstance(attachment, dict):
                        names.append(str(attachment.get("file_name", "?"))[:255])
                    else:
                        names.append("?")
                except Exception:
                    names.append("?")
            lines.append("    attachments: " + ", ".join(names))
            try:
                saved = core.save_received_attachments(payload.get("fields", {}), attachment_dir)
                for path in saved:
                    lines.append("    saved attachment: " + path)
            except Exception as error:
                lines.append("    attachment save error: " + str(error))
    elif event_type == "delivery_receipt":
        direction = "incoming" if payload.get("is_incoming") else "outgoing"
        lines.append(
            "delivery update " + direction + " " + str(payload.get("hash", "?")) +
            " state=" + str(payload.get("state", "?")) +
            " method=" + str(payload.get("method", "?"))
        )
    elif event_type == "announce":
        display_name = None
        try:
            dest_hash = payload.get("destination_hash")
            if isinstance(dest_hash, str) and dest_hash:
                peer = database.Announce.select().where(
                    database.Announce.destination_hash == dest_hash
                ).first()
                if peer is not None and peer.app_data:
                    display_name = parse_lxmf_display_name(peer.app_data, None)
        except Exception:
            display_name = None
        try:
            safe_name = str(display_name)[:64] if display_name else ""
        except Exception:
            safe_name = ""
        name_part = " (" + safe_name + ")" if safe_name else ""
        lines.append("announce " + str(payload.get("aspect", "?"))[:128] +
                     " from " + str(payload.get("destination_hash", "?"))[:64] + name_part)
    return lines


def printer_worker(core, events, attachment_dir):
    while True:
        try:
            event_type, payload = events.get(timeout=0.5)
        except queue.Empty:
            continue
        except Exception:
            continue
        try:
            lines = render_event(core, event_type, payload, attachment_dir)
            if lines:
                sys.stdout.write("\r\033[K")
                for line in lines:
                    print(line)
                print(PROMPT, end="", flush=True)
        except Exception as error:
            sys.stdout.write("\r\033[K")
            RNS.log("failed to render event " + str(event_type) + ": " + str(error), RNS.LOG_ERROR)
            print(PROMPT, end="", flush=True)
        events.task_done()

def print_help():
    print("available commands:")
    print("  /help")
    print("  /peers")
    print("  /announce [name]")
    print("  /msg <destination_hash> <text>")
    print("  /file <destination_hash> <path> (path must be inside a send directory, run /send_dir)")
    print("  /send_dir - show the directories that files may be sent from")
    print("  /history <destination_hash>")
    print("  /sync <propagation_node_destination_hash>")
    print("  /identity")
    print("  /quit")


def split_arguments(argument_text):
    argument_text = argument_text.strip()
    if not argument_text:
        return None, None
    first, separator, remaining = argument_text.partition(" ")
    if not separator:
        return first, None
    return first, remaining.strip()


def show_history(core, destination_hash):
    try:
        history = core.get_message_history(destination_hash)
    except ValueError as error:
        print("error: " + str(error))
        return
    except Exception as error:
        print("error: could not load history: " + str(error))
        return
    if not history:
        print("no messages with " + str(destination_hash)[:64])
        return
    for message in history:
        try:
            direction = "<=" if message.get("is_incoming") else "=>"
            time_string = format_timestamp(message.get("timestamp"))
            content = str(message.get("content", ""))[:2000]
            attachment_names = attachment_names_from_fields(message.get("fields"))
            suffix = ""
            if attachment_names:
                suffix = "  [attachments: " + ", ".join(attachment_names) + "]"
            print("[" + time_string + "] " + direction + " [" + str(message.get("state", "?")) + "] " + content + suffix)
        except Exception:
            continue


def handle_command(core, loop, attachment_dir, command_line):
    if not isinstance(command_line, str) or len(command_line) > 4 * 1024 * 1024:
        print("error: command too long or invalid")
        return True
    command_parts = command_line.split(maxsplit=1)
    if not command_parts:
        return True
    command = command_parts[0].lower()[:32]
    rest = command_parts[1] if len(command_parts) > 1 else ""
    first_argument, remaining_arguments = split_arguments(rest)

    if command == "/help":
        print_help()
    elif command == "/identity":
        try:
            print("identity hash: " + core.identity.hash.hex())
        except Exception:
            print("identity unavailable")
    elif command == "/peers":
        try:
            peers = core.get_peers()
        except Exception as error:
            print("error: could not list peers: " + str(error))
            return True
        if not peers:
            print("no peers discovered yet")
        for peer in peers:
            try:
                name = str(peer.get("display_name") or "-")[:64]
                aspect = str(peer.get("aspect", "?"))[:128]
                last_seen = format_timestamp(peer.get("updated_at"))
                print(str(peer.get("destination_hash", "?"))[:64] + "  " + name + "  " + aspect + "  last seen: " + last_seen)
            except Exception:
                continue
    elif command == "/announce":
        try:
            name = first_argument or core.config.display_name.get()
        except Exception:
            name = first_argument
        try:
            name = _validate_display_name(name)
        except Exception:
            name = "Anonymous Peer"
        try:
            core.config.display_name.set(name)
        except Exception as error:
            print("error: " + str(error))
            return True
        try:
            core.local_lxmf_destination.display_name = name.encode("utf-8")
        except Exception:
            try:
                core.local_lxmf_destination.display_name = name
            except Exception as error:
                print("error: could not set display name: " + str(error))
                return True
        try:
            core.announce()
        except Exception as error:
            print("error: " + str(error))
            return True
        print("announced as: " + name)
    elif command == "/send_dir":
        print(os.pathsep.join(core.get_send_directories()))
    elif command == "/msg":
        if first_argument is None or not remaining_arguments:
            print("usage: /msg <destination_hash> <text>")
        else:
            submit_to_loop(loop, core.send_message(first_argument, remaining_arguments))
            print("message queued to " + str(first_argument)[:64])
    elif command == "/file":
        if first_argument is None or not remaining_arguments:
            print("usage: /file <destination_hash> <path>")
        else:
            try:
                core.resolve_allowed_file_path(remaining_arguments)
            except ValueError as error:
                print("error: " + str(error))
            else:
                submit_to_loop(loop, core.send_file(first_argument, remaining_arguments))
                print("file transfer queued to " + str(first_argument)[:64])
    elif command == "/history":
        if first_argument is None:
            print("usage: /history <destination_hash>")
        else:
            show_history(core, first_argument)
    elif command == "/sync":
        if first_argument is None:
            print("usage: /sync <propagation_node_destination_hash>")
        else:
            try:
                core.sync_with_propagation_node(first_argument)
            except ValueError as error:
                print("error: " + str(error))
            except Exception as error:
                print("error: sync failed: " + str(error))
            else:
                print("sync requested from propagation node " + str(first_argument)[:64])
    elif command == "/quit":
        return False
    else:
        print("unknown command: " + command[:32] + " (try /help)")
    return True

def main():
    try:
        core = MeshChatCore()
    except Exception as error:
        print("failed to start core: " + str(error))
        sys.exit(1)

    events = queue.Queue()

    def on_core_event(event_type, payload):
        try:
            events.put_nowait((event_type, payload))
        except queue.Full:
            RNS.log("event queue is full, dropping event: " + str(event_type), RNS.LOG_WARNING)

    core.set_event_callback(on_core_event)

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_asyncio_loop, args=(loop,), daemon=True)
    loop_thread.start()

    attachment_dir = os.path.join(core.storage_path, "attachments")

    printer_thread = threading.Thread(target=printer_worker, args=(core, events, attachment_dir), daemon=True)
    printer_thread.start()

    submit_to_loop(loop, core.announce_loop())

    print("Reticulum MeshChat CLI")
    print("identity: " + core.identity.hash.hex())
    print("storage: " + core.storage_path)
    print("send directory: " + os.pathsep.join(core.get_send_directories()))
    print("type /help for a list of commands")

    exit_requested = False
    while not exit_requested:
        try:
            command_line = input(PROMPT)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            break
        except StopIteration:
            break
        command_line = command_line.strip()
        if not command_line:
            continue
        try:
            exit_requested = not handle_command(core, loop, attachment_dir, command_line)
        except KeyboardInterrupt:
            print()
            break
        except Exception as error:
            print("command error: " + str(error))

    print("shutting down...")
    try:
        loop.call_soon_threadsafe(loop.stop)
    except RuntimeError as error:
        RNS.log("failed to stop asyncio loop: " + str(error), RNS.LOG_WARNING)
    core.stop()
    print("bye")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print("bye")