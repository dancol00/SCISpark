package it.unisa.di.dif.test;

import it.unisa.di.dif.SCIManager;
import it.unisa.di.dif.pattern.Image;
import it.unisa.di.dif.pattern.ReferencePattern;
import it.unisa.di.dif.pattern.ResidualNoise;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestFileLoad {
    public static void main(String[] args) throws IOException {
        System.out.println("Extracting Residual Noise");
        ResidualNoise rn = SCIManager.extractResidualNoise("/Users/bruand/development/DatasetD90Reviewed/IC_190/imgs/i_51620.jpg");
//        System.out.println("Loading RP from npz");
//        File f = new File("/Users/bruand/IdeaProjects/SCILib/output/Z50/rp.txt");
//        ReferencePattern rp = ReferencePattern.load(f);
//        ResidualNoise rn = new ResidualNoise();
//        rn.initFromNpz("testRP.npz", "rn");
        System.out.println("Storing RN to NPz");
        Path output1 = Path.of("output", "testRP.npz");
        Path output2 = Path.of("output", "testRP2.npz");
        if(output1.toFile().exists()) output1.toFile().delete();
        if(output2.toFile().exists()) output2.toFile().delete();
        rn.storeAsNpz(output1, "rn");
        System.out.println("Stored");
        System.out.println("Loading RP from npz");
        ResidualNoise rn2 = new ResidualNoise();
        rn2.initFromNpz(output1, "rn");
        System.out.println("Loaded");
        System.out.println("Storing RN to NPz second file");
        rn2.storeAsNpz(output2, "rn");
        System.out.println("Stored");
//        f = new File("/Users/bruand/IdeaProjects/SCI_RP_Threshold_Java/results/rns/IC_191/i_51879.rn");
//        ResidualNoise rn = ResidualNoise.load(f);
//        System.out.println(rn);
    }
}
