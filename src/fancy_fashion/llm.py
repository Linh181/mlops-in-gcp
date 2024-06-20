
import vertexai
from vertexai.generative_models import GenerativeModel


def initialise_llm(gcp_project: str, vertexai_llm_location: str) -> GenerativeModel:
    vertexai.init(project=gcp_project, location=vertexai_llm_location)
    llm_model = GenerativeModel("gemini-pro")
    return llm_model


def generate_llm_response(prompt: str, llm_model: GenerativeModel) -> str:
    model_response = llm_model.generate_content(
        prompt,
        generation_config={"temperature": 0},
    )
    text = model_response.text
    
    return text
