# Not reliable enough.

from langchain_openai import ChatOpenAI
from browser_use import Agent
import asyncio
from dotenv import load_dotenv
load_dotenv()

task = """
Here a tripadvisor webpage with the list of hotels in Aspen:
https://www.tripadvisor.com/Hotels-g29141-Aspen_Colorado-Hotels.html

The page has part of the list of hotels. To view the full list, you need to:
- Please click out of the date selector.
- click on the "See All" button. This will show a few more hotels.
- then click on the "Next" button to see the next page of hotels.
- repeat until you have seen all hotels.

Each hotel has a url of the form:
https://www.tripadvisor.com/Hotel_Review-gXXX-dYYY-Reviews-ZZZZZ

Could you go through all 96 hotels and extract the YYY id for each hotel?

Please return the hotel ids in a json list.
"""

async def main():
    agent = Agent(
        task=task,
        llm=ChatOpenAI(model="gpt-4o"),
    )
    await agent.run()

asyncio.run(main())
