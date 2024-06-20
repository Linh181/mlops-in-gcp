from fastapi.testclient import TestClient

from fancy_fashion.app import app

client = TestClient(app)

def test_ping():
    response = client.get("/ping")
    assert response.status_code == 200  
    assert response.text == "pong"

def test_predict_endpoint():

    with open("./tests/data/0.jpg", "rb") as image_file:
        image_data = image_file.read()

        response = client.post("/predict", files={"image_data": ("0.jpg", image_data, "image/jpeg")})
        
        assert response.status_code == 200
        result = response.json()
        assert result["filename"] == "0.jpg"
        assert result["result"] in ["bag", "shirt", "sneaker", "dess", "trouser"]
