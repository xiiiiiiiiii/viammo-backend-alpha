# import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI()

input_text = "What is 2+2?"

response = client.responses.create(
  model="gpt-4o",
  input=[
    {
      "role": "user",
      "content": [
        {
          "type": "input_text",
          "text": input_text,
        }
      ]
    },
  ],
  text={
    "format": {
      "type": "text"
    }
  },
  reasoning={},
  tools=[],
  temperature=1,
  max_output_tokens=2048,
  top_p=1,
  store=True
)

print(response)
print()
print(response.output[0].content[0].text)
