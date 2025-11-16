import random
from typing import Dict
import torch
import torch.nn as nn
import torchvision.transforms as transforms
from torchvision import models
from typing import Dict
from PIL import Image
from app.logger import logger


CHEX_LABELS = [
    "Atelectasis", "Cardiomegaly", "Consolidation", "Edema",
    "Effusion", "Emphysema", "Fibrosis", "Infiltration",
    "Mass", "Nodule", "Pneumonia", "Pneumothorax",
    "Pleural Thickening", "Hernia"
]

class RealVisionModel:
    """Vision model using pretrained DenseNet121 (CheXNet-style)."""

    def __init__(self, device: str = "cpu"):
        logger.info("Initializing DenseNet121 for vision model.")
        self.device = device
        self.model = models.densenet121(weights=models.DenseNet121_Weights.DEFAULT)
        num_features = self.model.classifier.in_features
        # Replace classifier to match CheXNet-style 14 outputs
        self.model.classifier = nn.Sequential(
            nn.Linear(num_features, len(CHEX_LABELS)),
            nn.Sigmoid()
        )
        self.model.to(self.device)
        self.model.eval()

        # Define preprocessing pipeline
        self.transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.Grayscale(num_output_channels=3),  # replicate grayscale to 3 channels
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
        ])

    def preprocess(self, image: Image.Image) -> torch.Tensor:
        """Preprocess PIL image for model input."""
        return self.transform(image).unsqueeze(0).to(self.device)

    def predict(self, image: Image.Image) -> Dict[str, float]:
        """Run model inference and return label probabilities."""
        try:
            tensor = self.preprocess(image)
            with torch.no_grad():
                preds = self.model(tensor).cpu().numpy().flatten()
            results = {label: float(preds[i]) for i, label in enumerate(CHEX_LABELS)}
            logger.info("Vision model successfully generated predictions.")
            return results
        except Exception as e:
            logger.exception("Vision model inference failed.")
            return {}



# CHEX_LABELS = ["NORMAL", "PNEUMONIA"]

# class RealVisionModel:
#     def __init__(self, model_path="models/densenet121_binary.pth", device="cpu"):
#         self.device = torch.device(device)
#         self.model = models.densenet121(weights=None)
#         num_features = self.model.classifier.in_features
#         self.model.classifier = nn.Linear(num_features, len(CHEX_LABELS))
#         self.model.load_state_dict(torch.load(model_path, map_location=self.device))
#         self.model.eval()
#         self.model.to(self.device)

#         self.transform = transforms.Compose([
#             transforms.Resize((224, 224)),
#             transforms.ToTensor(),
#             transforms.Normalize([0.485, 0.456, 0.406],
#                                  [0.229, 0.224, 0.225])
#         ])
    
#     def predict(self, image_path):
#         if isinstance(image_path, str):
#             image = Image.open(image_path).convert("RGB")
#         elif isinstance(image_path, Image.Image):
#             image = image_path.convert("RGB")
#         else:
#             raise ValueError("Input must be a file path or a PIL.Image.Image")
        
#         image = self.transform(image).unsqueeze(0).to(self.device)
        
#         with torch.no_grad():
#             outputs = self.model(image)  # logits [1,2]
#             probs = torch.softmax(outputs, dim=1).cpu().numpy()[0]

#         findings = {CHEX_LABELS[i]: float(probs[i]) for i in range(len(CHEX_LABELS))}
#         return findings

