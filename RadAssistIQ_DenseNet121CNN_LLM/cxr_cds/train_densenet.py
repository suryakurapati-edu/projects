# train_densenet_binary.py
from multiprocessing import freeze_support
import os
import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, models, transforms
from torch.utils.data import DataLoader

def main():
    # Paths
    base_dir = "images"
    
    # Image transforms
    data_transforms = {
        "train": transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.RandomHorizontalFlip(),
            transforms.RandomRotation(10),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406],
                                 [0.229, 0.224, 0.225])
        ]),
        "val": transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406],
                                 [0.229, 0.224, 0.225])
        ]),
    }

    # Load datasets
    datasets_dict = {
        x: datasets.ImageFolder(os.path.join(base_dir, x), transform=data_transforms[x])
        for x in ["train", "val"]
    }

    # Dataloaders
    dataloaders = {
        x: DataLoader(datasets_dict[x], batch_size=16, shuffle=True, num_workers=2)
        for x in ["train", "val"]
    }

    # Initialize DenseNet121
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = models.densenet121(weights=models.DenseNet121_Weights.IMAGENET1K_V1)
    num_features = model.classifier.in_features
    model.classifier = nn.Linear(num_features, 2)  # binary classification
    model = model.to(device)

    # Loss and optimizer
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.0001)

    # Training loop
    num_epochs = 5
    for epoch in range(num_epochs):
        model.train()
        train_loss = 0.0

        for inputs, labels in dataloaders["train"]:
            inputs, labels = inputs.to(device), labels.to(device)
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()

        val_loss = 0.0
        model.eval()
        with torch.no_grad():
            for inputs, labels in dataloaders["val"]:
                inputs, labels = inputs.to(device), labels.to(device)
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                val_loss += loss.item()

        print(f"Epoch [{epoch+1}/{num_epochs}]  Train Loss: {train_loss/len(dataloaders['train']):.4f}  "
              f"Val Loss: {val_loss/len(dataloaders['val']):.4f}")

    # Save model
    os.makedirs("models", exist_ok=True)
    torch.save(model.state_dict(), "models/densenet121_binary.pth")
    print("✅ Model training complete and saved as models/densenet121_binary.pth")

if __name__ == "__main__":
    freeze_support()
    main()
