package com.spbsu.flamestream.example.bl.predicttest;

import com.spbsu.flamestream.example.bl.classifier.Predictor;
import org.apache.commons.io.FileUtils;
import org.jblas.DoubleMatrix;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static com.spbsu.flamestream.example.bl.classifier.Predictor.readLineDouble;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
    private static final String link = "https://www.dropbox.com/s/lu4b7e5w20xpho1/test_data?dl=1";
    private static File testData = null;

    @BeforeTest
    public void downloadSample() throws IOException {
        testData = new File("src/test/resources/test_data");

        if (!testData.exists()) {
            URL url = new URL(link);
            FileUtils.copyURLToFile(url, testData);
        }
    }

    @Test
    public void fiveDocumentTest() throws IOException {
        Predictor predictor = new Predictor();
        try (BufferedReader br = new BufferedReader(new FileReader(testData))) {
            // five python predictions provided by script
            for (int i = 0; i < 5; i++) {
                String line = br.readLine();
                double[] document = readLineDouble(line);

                line = br.readLine();
                double[] pyPrediction = readLineDouble(line);
                DoubleMatrix prediction = predictor.predictProba(document);

                boolean equals = true;
                assertEquals(prediction.length, pyPrediction.length);

                for (int j = 0; j < prediction.length; j++) {
                    double diff = abs(pyPrediction[j] - prediction.get(0, j));
                    if (diff > 5e-3) {
                        equals = false;
                        break;
                    }
                }

                assertTrue(equals);
            }
        }
    }
}
