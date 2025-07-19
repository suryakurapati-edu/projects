from transformers import Wav2Vec2ForCTC, Wav2Vec2Tokenizer
import torch
import librosa
import os

class AudioProcessor:
    def __init__(self):
        self.model_name = "facebook/wav2vec2-base-960h"
        self.tokenizer = Wav2Vec2Tokenizer.from_pretrained(self.model_name)
        self.model = Wav2Vec2ForCTC.from_pretrained(self.model_name)

    def transcribe(self, file_path: str) -> str:
        audio, rate = librosa.load(file_path, sr=16000, duration=180)
        input_values = self.tokenizer(audio, return_tensors="pt", padding="longest").input_values
        with torch.no_grad():
            logits = self.model(input_values).logits
        predicted_ids = torch.argmax(logits, dim=-1)
        transcription = self.tokenizer.batch_decode(predicted_ids)[0]
        return transcription

# if __name__=="__main__":
#     audio_path = "/Users/suryakurapati/Desktop/NCI/NCI-Notes/Deep Learning and Gen AI/Assignment/consultiq/data/ABC123.wav"
#     audio_processor = AudioProcessor()
#     transcription = audio_processor.transcribe(audio_path)
#     print(type(transcription))