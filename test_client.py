import requests
import sys

def main():
    server_url = "url"  # Adjust if deployed on Cloud Run or different host
    thread_id = None

    while True:
        user_input = input("Ask a baseball question (or type 'quit' to exit): ").strip()
        if user_input.lower() in ["quit", "exit"]:
            print("Goodbye.")
            break

        # If we don't have a thread_id yet, let's create one
        if not thread_id:
            create_resp = requests.post(f"{server_url}/create_thread")
            if create_resp.status_code != 200:
                print(f"Error calling /create_thread: {create_resp.text}")
                continue
            thread_id = create_resp.json().get("thread_id")
            print(f"[Client] Got thread_id={thread_id} from the server.")

        # Now we call /add_message with the user's question
        payload = {
            "thread_id": thread_id,
            "user_message": user_input
        }
        add_msg_resp = requests.post(f"{server_url}/add_message", json=payload)
        if add_msg_resp.status_code != 200:
            print(f"Error calling /add_message: {add_msg_resp.text}")
            continue

        data = add_msg_resp.json()
        # data is expected to be { "messages": [ { "role":"assistant" or "user", "text":"...", "image": ...}, ... ] }

        messages = data.get("messages", [])
        print("\n--- Agent Response ---")
        for i, msg in enumerate(messages):
            role = msg.get("role")
            text = msg.get("text", "")
            img_b64 = msg.get("image", None)

            print(f"{i+1}) role={role}\n   text={text}")
            if img_b64:
                # Just print the first ~50 characters of the base64 to confirm presence
                snippet = img_b64[:50] + "..."
                print(f"   (image base64 snippet)={snippet}")
        print("-----------------------\n")

if __name__ == "__main__":
    main()
