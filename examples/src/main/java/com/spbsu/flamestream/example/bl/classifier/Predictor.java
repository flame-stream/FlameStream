package com.spbsu.flamestream.example.bl.classifier;

import org.apache.commons.io.FileUtils;
import org.jblas.DoubleMatrix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import static org.jblas.MatrixFunctions.exp;

public class Predictor {
    private static final String url = "https://www.dropbox.com/s/t3w15pprjqlv4wl/meta_data?dl=1";
    private static final int sklearnFeatures = 371432;
    private final double[] intercept;
    private final DoubleMatrix weights;

    public Predictor() throws IOException {
        File metaData = new File("src/main/resources/meta_data");

        if (!metaData.exists()) {
            FileUtils.copyURLToFile(new URL(url), metaData);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(metaData))) {
            int classes = Integer.parseInt(br.readLine());
            double[][] inputCoef = new double[classes][sklearnFeatures];

            //int index = 0;
            String line;
            for (int index = 0; index < classes; index++) {
                line = br.readLine();
                double[] numbers = readLineDouble(line);

                assert numbers.length == sklearnFeatures;
                inputCoef[index] = numbers;
            }

            line = br.readLine();
            intercept = readLineDouble(line);
            // 87 371432
            //System.out.println(inputCoef.length + " " + inputCoef[0].length);

            weights = new DoubleMatrix(inputCoef).transpose();
        }
    }

    public static double[] readLineDouble(String line) {
        return Arrays
                .stream(line.split(" "))
                .mapToDouble(Double::parseDouble)
                .toArray();
    }

    // see _predict_proba_lr in base.py sklearn
    public DoubleMatrix predictProba(DoubleMatrix documents) {
        DoubleMatrix probabilities = decisionFunction(documents);

        probabilities = probabilities.mul(-1);
        probabilities = exp(probabilities);
        probabilities.add(1);

        for (int i = 0; i < probabilities.rows; i++) {
            for (int j = 0; j < probabilities.columns; j++) {
                double reciprocal = 1.0 / probabilities.get(i, j);
                probabilities.put(i, j, reciprocal);
            }
        }

        DoubleMatrix res;
        /*if (probabilities.rows == 1) { // not quite...
            DoubleMatrix top = probabilities.mul(-1).add(1);
            res = DoubleMatrix.concatVertically(top, probabilities).transpose();
        }*/

        double[] vector = new double[probabilities.rows];
        DoubleMatrix sums = probabilities.rowSums();
        for (int i = 0; i < probabilities.rows; i++) {
            vector[i] = sums.get(i, 0);
        }

        double[][] matrix = new double[1][probabilities.rows];
        matrix[0] = vector;
        DoubleMatrix denominator = new DoubleMatrix(matrix);
        denominator.reshape(probabilities.rows, probabilities.rows);

        res = probabilities.div(denominator);

        return res;
    }

    public DoubleMatrix predictProba(double[] document) {
        return predictProba(new DoubleMatrix(document));
    }

    private DoubleMatrix decisionFunction(double[] document) {
        return decisionFunction(new DoubleMatrix(document));
    }

    private DoubleMatrix decisionFunction(DoubleMatrix documents) {
        DoubleMatrix inter = new DoubleMatrix(1, intercept.length, intercept);

        DoubleMatrix score = documents.transpose().mmul(weights);
        return score.add(inter);
    }
}
