import asyncio
import json
from typing import Any, Dict, List, Optional
import io

from Agents.AsyncSQLQueryGeneratorAgent import AsyncSQLQueryGeneratorAgent
from Agents.VisualizationAgent import VisualizationAgent

class AnalystAgent:
    """
    Uses an existing Analyst Assistant (in the Assistants API) to:
      - Start a run on a thread that has the user's last message.
      - If the agent calls some tools, we parse the calls, run them in parallel,
        cancel the run, and post their outputs to the thread.
      - Then we do a second run (tool_choice="none") so the agent can finalize its answer.
    """

    def __init__(self, openai_client, analyst_assistant_id: str, query_agent: AsyncSQLQueryGeneratorAgent, viz_agent: VisualizationAgent):
        """
        :param openai_client: The async client with Beta endpoints (e.g., AsyncOpenAI)
        :param analyst_assistant_id: ID of the previously created "Analyst Assistant"
        """
        self.client = openai_client
        self.analyst_assistant_id = analyst_assistant_id
        self.query_agent = query_agent
        self.viz_agent = viz_agent

    async def handle_query(self, thread_id: str) -> None:
        """
        1) Start a run with create_and_poll.
        2) If the run calls tools, gather them in parallel, then cancel the run.
        3) Post the tool results to the thread.
        4) Start a second run with tool_choice="none" to finalize the answer.
        """
        # 1) Start the first run
        print(f"[AnalystAgent] Starting first run on thread {thread_id}...")
        run1 = await self.client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=self.analyst_assistant_id,
            poll_interval_ms=2000
        )
        print(f"[AnalystAgent] run1 ended with status={run1.status}")

        # 2) If run completed with no tool calls, done
        if run1.status == "completed" and not run1.required_action:
            print("[AnalystAgent] The agent answered without tools. Done.")
            return

        # 3) If the run is waiting for tool outputs:
        if run1.status == "requires_action":
            required = run1.required_action
            if required.type == "submit_tool_outputs":
                tool_calls = required.submit_tool_outputs.tool_calls
                print(f"[AnalystAgent] The agent called {len(tool_calls)} tool(s). We'll handle them now.")
                print(tool_calls)

                # Cancel this run so we can attach messages ourselves
                await self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run1.id)

                # Execute each tool call in parallel, then post the results
                await self._handle_tool_calls_in_parallel(thread_id, tool_calls)

                # Final run with tool_choice="none" to produce the final answer
                run2 = await self.client.beta.threads.runs.create_and_poll(
                    thread_id=thread_id,
                    assistant_id=self.analyst_assistant_id,
                    poll_interval_ms=2000,
                    tool_choice="none",  # no more calls
                    additional_instructions="\nIMPORTANT: We have already handled the tool calls for the above user query, and added the results of the tools above. Use those results in your response to the user query."
                )
                print(f"[AnalystAgent] run2 ended with status={run2.status}")
            else:
                print(f"[AnalystAgent] The run requires some other action: {required.type}")
        else:
            print(f"[AnalystAgent] The run ended with status={run1.status}, no further steps to do.")

    async def _handle_tool_calls_in_parallel(self, thread_id: str, tool_calls: List):
        """
        Parse each function call. We'll run them all concurrently with asyncio.gather,
        then post each result to the thread.
        """
        # 1) Build tasks for each tool call
        tasks = []
        call_metas = []  # store (index, call) so we can map results back

        for i, call in enumerate(tool_calls):
            func_name = call.function.name
            func_args_str = call.function.arguments
            args_dict = json.loads(func_args_str)

            # Build a task for each call
            if func_name == "statcast_query":
                # We'll run the sql query agent in a separate async method
                tasks.append(self._run_sql_tool(args_dict))
                call_metas.append((i, func_name))
            elif func_name == "visualization_tool":
                tasks.append(self._run_viz_tool(args_dict))
                call_metas.append((i, func_name))
            else:
                # unrecognized tool
                tasks.append(asyncio.sleep(0))  # no-op
                call_metas.append((i, func_name))

        # 2) run them all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 'results' is a list of same length as tool_calls
        # each item is either a string (data) or a dict or an image path, etc.
        # depending on your stubs.
        # We'll now post them in the same order.
        for (i, func_name), output in zip(call_metas, results):
            if isinstance(output, Exception):
                # handle error
                content = f"[Tool] Tool {func_name} was not able to be completed at this time"
                role = "assistant"
            else:
                # normal result
                if func_name == "statcast_query":
                    # output is a string (like CSV or JSON)
                    content = f"[Tool] Here is the data from statcast_query tool to help you answer my query:\n"
                    for out in output: # could be multiple queries
                        content += f"\n{out['query_description']}:\n{out['data']}"
                    role = "user"
                elif func_name == "visualization_tool":
                    # output is a file_id
                    content = None
                    role = "user"
                else:
                    content = f"[Tool] Unrecognized tool result: {output}"
                    role = "user"

            if func_name == "visualization_tool" and not isinstance(output, Exception):
                # Output will be the image file id, it has already been uploaded


                # Here the image was already created in a code_interpreter run, but it does not specify the mime type, which causes an error if you directly use that image id
                # Because of this, we need to recreate the image with the proper metadata. Dumb but necessary.
                image_file_id = output
                #print("imageid: " + image_file_id)
                file_resp = await self.client.files.content(image_file_id)

                file_bytes = file_resp.read()
                new_file = io.BytesIO(file_bytes)
                new_file.name = "visualization.png"

                new_file_resp = await self.client.files.create(
                    file=new_file,
                    purpose="vision"
                )
                new_file_id = new_file_resp.id

                await self.client.beta.threads.messages.create(
                    thread_id=thread_id,
                    role=role,
                    content=[
                        {
                            "type": "text",
                            "text": "[Tool] Here is the chart from the visualization tool to help you answer my query."
                        },
                        {
                            "type": "image_file",
                            "image_file": {
                                "file_id": new_file_id  
                            }
                        },
                    ]
                )
            else:
                # for the SQL data or an error
                await self.client.beta.threads.messages.create(
                    thread_id=thread_id,
                    role=role,
                    content=content if content else "[Tool] No content (error)"
                )

    # Stub or calls to your real local code
    async def _run_sql_tool(self, args_dict: Dict[str, Any]) -> str:
        """
        e.g. local call to your AsyncSQLQueryGeneratorAgent
        { "query_descriptions": ["Which pitcher had highest release speed?"] }
        """
        query_descs = args_dict["query_descriptions"]

        res = await self.query_agent.generate_and_run_queries(query_descs)
        return res

    async def _run_viz_tool(self, args_dict: Dict[str, Any]) -> str:
        """
        e.g. local call to VisualizationAgent
        { "visualization_description": "Trend line of homers" }
        """
        desc = args_dict["visualization_description"]
     
        res = await self.viz_agent.create_visualization(desc)
        return res #image id
