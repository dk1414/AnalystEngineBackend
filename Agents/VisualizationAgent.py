import io
from typing import Optional, Dict, List

from Agents.AsyncSQLQueryGeneratorAgent import AsyncSQLQueryGeneratorAgent

class VisualizationAgent:
    """
    A class to handle the creation of data visualizations by:
      1) Using the AsyncSQLQueryGeneratorAgent to gather data as CSV.
      2) Creating a new thread in the Visualization Assistant (with code_interpreter).
      3) Uploading the CSV file, describing the chart we want.
      4) Running the thread and waiting for completion (code interpreter).
      5) Downloading the final image from the Assistant's output.
    """

    def __init__(
        self,
        async_sql_query_agent: AsyncSQLQueryGeneratorAgent,
        openai_client,  # The async client with Beta endpoints for Assistants (AsyncOpenAI)
        visualization_assistant_id: str,
        csv_schema_mapping: Optional[Dict[str, str]] = None,
    ):
        """
        :param async_sql_query_agent: instance of AsyncSQLQueryGeneratorAgent
        :param openai_client:   The Assistants API client (Beta), supporting async
        :param visualization_assistant_id: The ID for your "Visualization Assistant"
                                           that has the code_interpreter tool.
        :param csv_schema_mapping: optional dict mapping {column_name -> short_description}
                                   used to build a user-friendly description of CSV columns.
        """
        self.async_sql_query_agent = async_sql_query_agent
        self.client = openai_client
        self.visualization_assistant_id = visualization_assistant_id
        self.csv_schema_mapping = csv_schema_mapping or {}

    async def create_visualization(self, visualization_description: str) -> str:
        """
        Main method to:
         1) Ask SQL Query Agent for the needed data (CSV).
         2) Create a new thread with the Visualization Assistant.
         3) Upload the CSV, add a user message describing the chart.
         4) Start a run, wait for completion.
         5) Download the final image, return the local path.

        :param visualization_description: e.g. "Trend line showing home run numbers per season"
        :return: Local path to the final image (or some unique handle).
        """

        # Step 1: Gather data from the Async SQL Query Agent as CSV
        query_prompt = f"Gather the data necessary for making this visualization: {visualization_description}"

        # generate_and_run_queries returns a list of dicts
        # e.g. [ { "query_description": ..., "generated_sql": ..., "data": <CSV_STRING> }, ... ]
        #print("getting data")
        results = await self.async_sql_query_agent.generate_and_run_queries([query_prompt])
        #print(results[0]['generated_sql'], results[0]['error'])
        csv_data = results[0]["data"]
        if not csv_data:
            raise RuntimeError("No CSV data returned from SQL agent. Can't proceed with visualization.")

        # Step 1.1: Check CSV size (must be < 512 MB)
        csv_bytes = csv_data.encode("utf-8")
        if len(csv_bytes) > 512 * 1024 * 1024:
            raise RuntimeError("CSV data exceeds 512 MB limit for file attachments.")
        
        #print(csv_data[0:100])

        # (Optional) Step 4: parse CSV columns & build a descriptive user message
        # columns = self._extract_csv_columns(csv_data)
        # columns_desc = self._build_columns_desc(columns)
        # We might incorporate columns_desc into the user_msg_content if desired.

        # Step 2: Create a new thread with the Visualization Assistant
        #print('Creating thread')
        new_thread = await self.client.beta.threads.create()
        thread_id = new_thread.id
        #print(f'Thread id: {thread_id}')

        try:

            #print("creating csv file")
            # Step 3: Upload the CSV file to the thread
            csv_file_id = await self._upload_csv_to_thread(thread_id, csv_bytes)

            # Step 4: Build user message describing the chart (plus any column info if desired)
            user_msg_content = (
                f"Please create a visualization: {visualization_description}.\n"
                "I've attached CSV data for you to use in your code interpreter.\n"
            )

            #print("adding message and csv file to thread")

            # Step 4.1: Add the user message with the CSV file attached
            await self.client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=user_msg_content,
                attachments=[
                    {
                        "file_id": csv_file_id,
                        "tools": [{"type": "code_interpreter"}]
                    }
                ]
            )

            #print("running thread")

            # Step 5: Start a Run with the Visualization Assistant
            run = await self.client.beta.threads.runs.create_and_poll(
                thread_id=thread_id,
                assistant_id=self.visualization_assistant_id,
                poll_interval_ms=2000  # Poll every 2 seconds
            )
            if run.status != "completed":
                raise RuntimeError(f"Run did not complete successfully: {run}")
            
            #print(run)

            #print("looking for image id")
            # Step 6: Find and download the final image
            image_file_id = await self._extract_image_file_id_from_run(thread_id, run.id)
            if not image_file_id:
                raise RuntimeError("No image file found in final run. Possibly no chart produced?")

            #print("downloading file")
            #local_img_path = await self._download_file(image_file_id, "final_plot.png")

            #delete thread
            await self.client.beta.threads.delete(thread_id)

            return image_file_id
        
        except Exception as e:
            #delete thread
            await self.client.beta.threads.delete(thread_id)
            raise(e)

    # ----------------------------------------------------------------
    # Internal helper methods
    # ----------------------------------------------------------------
    async def _upload_csv_to_thread(self, thread_id: str, csv_bytes: bytes) -> str:
        """
        Upload CSV data (as bytes) as a file to the Assistants API
        and return the newly created file ID.
        """
        file_resp = await self.client.files.create(
            file=io.BytesIO(csv_bytes),
            purpose="assistants"
        )
        return file_resp.id


    async def _extract_image_file_id_from_run(self, thread_id: str, run_id: str) -> Optional[str]:
        """
        Inspect the final run steps or messages for an image file_id 
        produced by the code interpreter tool.
        """

        #print(f"Steps list: {steps_list}")

        # Check the last messages in the thread
        messages_page = await self.client.beta.threads.messages.list(thread_id=thread_id)
        messages_list = messages_page.data  # This is a list of Message objects
        #print(messages_list)

        image_file_id = None

        # We iterate from last message to first (the newest is typically at the end)
        for msg in reversed(messages_list):
            # msg.content is a list of ContentBlock (ImageFileContentBlock, TextContentBlock, etc.)
            for block in msg.content:
                if block.type == "image_file":
                    image_file_id = block.image_file.file_id
                    break

            if image_file_id:
                break

        return image_file_id

    async def _download_file(self, file_id: str, local_name: str) -> str:
        """
        Download a file (like the resulting image) from the Assistants API
        in an async manner and save to local_name. Return local_name.
        """
        file_resp = await self.client.files.content(file_id)
        file_bytes = file_resp.read()
        with open(local_name, "wb") as f:
            f.write(file_bytes)
        return local_name

    # (Optional)
    def _extract_csv_columns(self, csv_data: str) -> List[str]:
        """
        Basic utility to parse the first line of CSV to get column headers.
        """
        lines = csv_data.strip().split("\n")
        headers = lines[0].split(",")
        return [h.strip() for h in headers]

    def _build_columns_desc(self, columns: List[str]) -> str:
        """
        Build a text describing each column from self.csv_schema_mapping if available.
        """
        desc_lines = []
        for col in columns:
            desc = self.csv_schema_mapping.get(col, "No description found.")
            desc_lines.append(f"- {col}: {desc}")
        return "\n".join(desc_lines)
