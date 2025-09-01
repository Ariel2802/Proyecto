import lightning as L
import torch
import torch.nn.functional as F
import torchmetrics
from torchmetrics.classification import (
    BinaryAccuracy, BinaryF1Score, BinaryAUROC
)

class LightningModel(L.LightningModule):
    def __init__(self, model, learning_rate=1e-3, num_classes=2, class_weights=None):
        super().__init__()
        self.learning_rate = learning_rate
        self.model = model
        self.num_classes = num_classes
        self.save_hyperparameters(ignore=["model"])

        # ===== métricas =====
        if num_classes == 2:
            # binario (tu caso)
            self.train_acc = BinaryAccuracy()
            self.val_acc   = BinaryAccuracy()
            self.val_f1    = BinaryF1Score()
            self.val_auc   = BinaryAUROC()
            self.test_acc  = BinaryAccuracy()
            self.test_f1   = BinaryF1Score()
            self.test_auc  = BinaryAUROC()
        else:
            # fallback multiclass (por si reusas el módulo)
            self.train_acc = torchmetrics.Accuracy(task="multiclass", num_classes=num_classes)
            self.val_acc   = torchmetrics.Accuracy(task="multiclass", num_classes=num_classes)
            self.test_acc  = torchmetrics.Accuracy(task="multiclass", num_classes=num_classes)
            self.val_f1 = self.val_auc = self.test_f1 = self.test_auc = None  # no usadas

        if class_weights is not None:
            self.register_buffer("class_weights", class_weights.float())
        else:
            self.class_weights = None

    def forward(self, x):
        return self.model(x)

    def _loss(self, logits, y):
        if self.class_weights is not None:
            return F.cross_entropy(logits, y, weight=self.class_weights)
        return F.cross_entropy(logits, y)

    def training_step(self, batch, _):
        x, y = batch
        logits = self(x)                     # [B, num_classes]
        loss = self._loss(logits, y)
        preds = logits.argmax(dim=1)

        self.train_acc.update(preds, y)
        self.log("train_loss", loss, prog_bar=True)
        self.log("train_acc",  self.train_acc, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, _):
        x, y = batch
        logits = self(x)
        loss = self._loss(logits, y)
        preds = logits.argmax(dim=1)
        self.val_acc.update(preds, y)
        self.log("val_loss", loss, prog_bar=True)
        self.log("val_acc",  self.val_acc, on_epoch=True, prog_bar=True)

        # sólo para binario
        if self.num_classes == 2:
            probs_pos = logits.softmax(dim=1)[:, 1]
            self.val_f1.update(preds, y)
            self.val_auc.update(probs_pos, y)
            self.log("val_f1",  self.val_f1,  on_epoch=True, prog_bar=True)
            self.log("val_auc", self.val_auc, on_epoch=True, prog_bar=True)

    def test_step(self, batch, _):
        x, y = batch
        logits = self(x)
        preds = logits.argmax(dim=1)
        self.test_acc.update(preds, y)
        self.log("test_acc", self.test_acc, on_epoch=True, prog_bar=True)

        if self.num_classes == 2:
            probs_pos = logits.softmax(dim=1)[:, 1]
            self.test_f1.update(preds, y)
            self.test_auc.update(probs_pos, y)
            self.log("test_f1",  self.test_f1,  on_epoch=True)
            self.log("test_auc", self.test_auc, on_epoch=True)

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)
