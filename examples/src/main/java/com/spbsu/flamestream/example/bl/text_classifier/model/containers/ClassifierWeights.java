package com.spbsu.flamestream.example.bl.text_classifier.model.containers;

import com.expleague.commons.math.vectors.Mx;

public class ClassifierWeights implements ClassifierInput, ClassifierOutput {
    private final String weights;

    public ClassifierWeights(String weights) {
        this.weights = weights;
    }

    public String getWeights() {
        return weights;
    }

}
