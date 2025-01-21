import os
import asyncio
from quart import Quart, request, jsonify
from typing import Optional
import base64

from dotenv import load_dotenv
load_dotenv()

# Example placeholders for your actual classes
from openai import AsyncOpenAI
from asyncutils.async_db_connection import get_async_engine
from Agents.AsyncSQLQueryGeneratorAgent import AsyncSQLQueryGeneratorAgent
from Agents.VisualizationAgent import VisualizationAgent
from Agents.AnalystAgent import AnalystAgent

app = Quart(__name__)

# Global references
openai_client: Optional[AsyncOpenAI] = None
db_engine = None
sql_agent: Optional[AsyncSQLQueryGeneratorAgent] = None
viz_agent: Optional[VisualizationAgent] = None
analyst_agent: Optional[AnalystAgent] = None

@app.before_serving
async def startup():
    """
    Called once when the server starts (before handling requests).
    Great place to do async initialization for DB engine, agent objects, etc.
    """
    global openai_client, db_engine, sql_agent, viz_agent, analyst_agent

    # 1) Setup openai_client
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    openai_client = AsyncOpenAI(api_key=openai_api_key)
    print("[startup] OpenAI client created")

    # 2) Setup db engine
    db_engine = await get_async_engine("statcast")
    print("[startup] DB engine created for statcast")

    # 3) Create the SQL Query Agent
    sql_agent = AsyncSQLQueryGeneratorAgent(
        client=openai_client,
        engine=db_engine,
        openai_model="o1-mini",
        output_format='csv'
    )
    print("[startup] SQL Query Agent created")

    # 4) Create the Visualization Agent
    # Suppose you have an existing 'viz_asst_id'
    viz_asst_id = os.getenv("VIZ_ASSIST_ID", "")

    viz_agent = VisualizationAgent(
        async_sql_query_agent=sql_agent,
        openai_client=openai_client,
        visualization_assistant_id=viz_asst_id
    )
    print("[startup] Visualization Agent created")

    # 5) Create your "Analyst Assistant"
    # Suppose we already have an ID for it
    analyst_asst_id = os.getenv("ANALYST_ASSIST_ID")
    analyst_agent = AnalystAgent(
        openai_client=openai_client,
        analyst_assistant_id=analyst_asst_id,
        query_agent=sql_agent,
        viz_agent=viz_agent
    )
    print("[startup] Analyst Agent created")

    print("[startup] All agents and engine are initialized.")

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'}), 200

@app.route("/create_thread", methods=["POST"])
async def create_thread():
    """
    Creates a new thread in the Assistants API and returns thread_id in JSON.
    """
    global openai_client
    new_thread = await openai_client.beta.threads.create()
    thread_id = new_thread.id
    print(f"[create_thread] Created new thread {thread_id}")
    return jsonify({"thread_id": thread_id}), 200

@app.route("/add_message", methods=["POST"])
async def add_message():
    """
    1) Read JSON: { "thread_id": str, "user_message": str }
    2) Add user message to that thread
    3) Let analyst_agent handle the query
    4) Parse final assistant response from the thread
    5) Return final answer (with any images) as JSON
    """
    global openai_client, analyst_agent

    data = await request.get_json()
    if not data or "thread_id" not in data or "user_message" not in data:
        return jsonify({"error": "Missing thread_id or user_message"}), 400

    thread_id = data["thread_id"]
    user_message = data["user_message"]

    # Step 1: Add user message
    user_msg = await openai_client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=user_message
    )
    print(f"[add_message] User message added to thread={thread_id}, msg_id={user_msg.id}")

    # Step 2: Let the AnalystAgent handle the query
    await analyst_agent.handle_query(thread_id)

    # Step 3: Retrieve final assistant messages
    msgs_page = await openai_client.beta.threads.messages.list(thread_id=thread_id)
    msgs_list = msgs_page.data

    # We'll iterate from last to first (the newest message to oldest).
    # We'll keep track of everything until we see a user message
    # whose text doesn't begin with "[Tool]".
    print(msgs_list)
    collected_messages = []
    for msg in msgs_list:
        if msg.role == "assistant":
            # This is presumably the final answer from the agent.
            # We want to keep this message in the final output.
            # Parse out text blocks, image blocks if any.
            parsed = await parse_message(msg)
            collected_messages.append(parsed)

        elif msg.role == "user":
            # Check the text blocks. If the first text block (or any block)
            # starts with "[Tool]", then it's a tool output message, 
            # so we also want to keep it (since it might have images).
            # If it doesn't start with "[Tool]", that means we
            # have found the original user query => break.
            is_tool_msg = False
            if msg.content:
                first_block = msg.content[0]
                if first_block.type == "text":
                    if first_block.text.value.startswith("[Tool]"):
                        # This is a tool message. We keep it and possibly parse images.
                        parsed = await parse_message(msg)
                        if parsed['image']:
                            collected_messages.append(parsed)
                        is_tool_msg = True
            
            if not is_tool_msg:
                # This is the real user's query => stop collecting further.
                break



    collected_messages.reverse()

    print(collected_messages)

    # Return them as JSON
    return jsonify({"messages": collected_messages})

async def parse_message(msg):
    """
    Returns something like:
    {
      "role": "assistant" or "user",
      "text": "some text combined",
      "images": [ { "file_id": ..., "base64": ... } ... ]
    }
    """
    text_buf = ''
    image = None

    if msg.content:
        for block in msg.content:
            if block.type == "text":
                text_buf = block.text.value
            elif block.type == "image_file":
                # we have an image file
                file_id = block.image_file.file_id
                # Download the image if you want base64
                file_resp = await openai_client.files.content(file_id)
                image_bytes = file_resp.read()

                image = base64.b64encode(image_bytes).decode("utf-8")


    return {
        "role": msg.role,
        "text": text_buf,
        "image": image
    }


if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))

