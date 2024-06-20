import numpy as np
import tensorflow as tf
from tensorflow.keras import Model, layers, preprocessing
from tensorflow.keras.applications import MobileNet


LABEL_MAPPING = {
    0: "bag",
    1: "shirt",
    2: "sneaker",
    3: "dress",
    4: "trouser",
}
REVERSE_LABEL_MAPPING = {v: k for k, v in LABEL_MAPPING.items()}


def train_model(train_data_dir):
    """Trains the model on the given training data."""

    tf.config.run_functions_eagerly(True)

    model = build_model()
    model.compile(
        optimizer="Adam", loss="categorical_crossentropy", metrics=["accuracy"]
    )

    train_data = preprocessing.image_dataset_from_directory(
        train_data_dir, label_mode="categorical", image_size=(128, 128)
    )
    model.fit(train_data, verbose=1, epochs=2, steps_per_epoch=10)

    return model


def build_model():
    """Builds the model structure."""

    # Start with basic MobileNet model
    base_model = MobileNet(include_top=False, input_shape=(128, 128, 3))
    for layer in base_model.layers:
        layer.trainable = False

    # Add extra leyers
    x = base_model.output
    x = layers.GlobalAveragePooling2D()(x)
    x = layers.Dense(units=512, activation="relu")(x)

    # Add output layers (3 classes: bags, shoes, sneakers)
    predictions = layers.Dense(units=3, activation="softmax")(x)
    model = Model(inputs=base_model.input, outputs=predictions)

    return model


def evaluate_model(model, test_data_dir):
    """Evaluates a given model on a test dataset."""

    test_datagenerator = preprocessing.image_dataset_from_directory(
        test_data_dir, label_mode="categorical", image_size=(128, 128)
    )
    loss, accuracy = model.evaluate(test_datagenerator)

    return loss, accuracy


def generate_predictions(model, predict_data_dir):
    """Generates predictions for a dataset with the given model."""

    predict_data = preprocessing.image_dataset_from_directory(
        predict_data_dir,
        label_mode=None,
        image_size=(128, 128),
        shuffle=False,
    )
    predictions = model.predict(predict_data)

    return [
        {"filename": filename.split("/")[-1], "prediction": pred}
        for filename, pred in zip(predict_data.file_paths, predictions.argmax(axis=1))
    ]


def generate_prediction(model, image_file):
    """Generates prediction for image data provided by a post api request"""

    img = preprocessing.image.load_img(image_file, target_size=(128, 128))
    # Convert image to numpy array
    predict_data = preprocessing.image.img_to_array(img)
    # Convert to batched dataset (array of images)
    predict_data = np.expand_dims(predict_data, axis=0)
    prediction = model.predict(predict_data)

    return prediction
