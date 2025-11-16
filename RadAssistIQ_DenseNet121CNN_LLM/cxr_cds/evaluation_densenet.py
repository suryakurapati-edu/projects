import os
import torch
import torch.nn as nn
from torchvision import datasets, models, transforms
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import numpy as np
import pandas as pd

# ------------------------------
# CONFIGURATION
# ------------------------------
TEST_DIR = "images/test"  # Path to your test images
MODEL_PATH = "models/densenet121_binary.pth"  # Path to trained model
BATCH_SIZE = 16
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# ------------------------------
# DATA TRANSFORMS
# ------------------------------
test_transforms = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406],
                         [0.229, 0.224, 0.225])
])

# ------------------------------
# LOAD TEST DATASET
# ------------------------------
test_dataset = datasets.ImageFolder(TEST_DIR, transform=test_transforms)
test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=BATCH_SIZE, shuffle=False)

class_names = test_dataset.classes
print(f"Loaded {len(test_dataset)} test images across {len(class_names)} classes: {class_names}")

# ------------------------------
# LOAD MODEL
# ------------------------------
model = models.densenet121(pretrained=False)
num_features = model.classifier.in_features
model.classifier = nn.Linear(num_features, len(class_names))
model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
model = model.to(DEVICE)
model.eval()

# ------------------------------
# PREDICTION LOOP
# ------------------------------
y_true = []
y_pred = []

with torch.no_grad():
    for inputs, labels in test_loader:
        inputs, labels = inputs.to(DEVICE), labels.to(DEVICE)
        outputs = model(inputs)
        _, preds = torch.max(outputs, 1)
        y_true.extend(labels.cpu().numpy())
        y_pred.extend(preds.cpu().numpy())

# ------------------------------
# EVALUATION METRICS
# ------------------------------
report = classification_report(y_true, y_pred, target_names=class_names, output_dict=True)
conf_matrix = confusion_matrix(y_true, y_pred)
accuracy = accuracy_score(y_true, y_pred)

# Convert to readable DataFrame
report_df = pd.DataFrame(report).transpose()
print("\nClassification Report:")
print(report_df)
print("\nConfusion Matrix:")
print(conf_matrix)
print(f"\nOverall Accuracy: {accuracy:.4f}")

# # ------------------------------
# # SAVE REPORT
# # ------------------------------
# os.makedirs("evaluation", exist_ok=True)
# report_df.to_csv("evaluation/classification_report.csv", index=True)
# np.savetxt("evaluation/confusion_matrix.csv", conf_matrix, delimiter=",", fmt="%d")

# print("\n Evaluation complete! Reports savsed under /evaluation/")
