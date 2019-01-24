package com.spbsu.flamestream.example.bl.classifier;

interface TopicsPredictor {
    Topic[] predict(Document document);
}
