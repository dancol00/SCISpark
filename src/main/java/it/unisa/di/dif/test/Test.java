package it.unisa.di.dif.test;

import it.unisa.di.dif.SCIManager;
import it.unisa.di.dif.pattern.Image;
import it.unisa.di.dif.pattern.ReferencePattern;
import it.unisa.di.dif.pattern.ResidualNoise;
import it.unisa.di.dif.utils.AdaptationMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
class RPTuple {
    public String device;
    public ReferencePattern pattern;

    public RPTuple(String device, ReferencePattern pattern) {
        this.device = device;
        this.pattern = pattern;
    }
}
class RNTuple {
    public String device;
    public String imgName;
    public ResidualNoise noise;

    public RNTuple(String device, String imgName, ResidualNoise noise) {
        this.device = device;
        this.imgName = imgName;
        this.noise = noise;
    }
}

class ImageTuple {
    public String device;
    public String imgName;
    public Image image;

    public ImageTuple(String device, String imgName, Image image) {
        this.device = device;
        this.imgName = imgName;
        this.image = image;
    }
}

public class Test {
    public static ReferencePattern getRPFromFolder(String folderPath) throws IOException {
        System.out.println("RP for: " + folderPath);
        List<Path> result =  null;
        try (Stream<Path> walk = Files.walk(Paths.get(folderPath))) {
            result = walk.filter(p->!Files.isDirectory(p))
                    .filter(p->p.toString().toLowerCase().endsWith(".jpg"))
                    .collect(Collectors.toList());

        }
//        ArrayList<Image> images = result.stream().map(s->{
//            try {
//                return new Image(s);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }).collect(Collectors.toCollection(ArrayList::new));

        return SCIManager.extractReferencePattern(result);
    }

    public static RNTuple[] getRNFromFolder(String folderPath, String device) throws IOException {
        System.out.println("RN for: " + folderPath);
        List<String> result = null;
        try (Stream<Path> walk = Files.walk(Paths.get(folderPath))) {
            result = walk.filter(p->!Files.isDirectory(p))
                    .map(Path::toString)
                    .filter(p->p.toLowerCase().endsWith(".jpg"))
                    .collect(Collectors.toList());

        }
        RNTuple[] rval = null;

        ArrayList<ImageTuple> images = result.stream().map(s->{
            try {
                return new ImageTuple(device, s, new Image(s));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(ArrayList::new));

        rval = images.stream().map(img -> {
            return new RNTuple(img.device, img.imgName, SCIManager.extractResidualNoise(img.image));
        }).toArray(RNTuple[]::new);
        return rval;
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("loglevel", "DEBUG");
        String basePath = "data";
        String baseOutputPath = "output";
        String[] devicesPath = {"D90_1", "D90_2", "D850", "Z50"};
        ArrayList<String> devices = new ArrayList<>(Arrays.asList(devicesPath));
        //Extract reference patterns
        ArrayList<RPTuple> rps = devices.stream().map(s->{
            try {
                Path p = Paths.get(basePath, s, "training");
                return new RPTuple(s, getRPFromFolder(p.toAbsolutePath().toString()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(ArrayList::new));

        for (RPTuple rp : rps) {
            // Store Reference Pattern as text file with float values
            Path p = Paths.get(baseOutputPath, rp.device, "rp.txt");
            File f = new File(p.toAbsolutePath().toString());
            f.getParentFile().mkdirs();
            rp.pattern.storeAsFloat(f);

            // Print Reference Pattern infos
            System.out.println(rp.device + ": " + rp.pattern.toString());
        }

        //Extract residual noises
        ArrayList<RNTuple> rns = devices.stream().map(s->{
            try {
                Path p = Paths.get(basePath, s, "testing");
                return getRNFromFolder(p.toAbsolutePath().toString(), s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).flatMap(Arrays::stream).collect(Collectors.toCollection(ArrayList::new));

        for (RNTuple rn : rns) {
            // Store Residual Noise as text file with float values
            String imgname = new File(rn.imgName).getName();
            Path p = Paths.get(baseOutputPath, rn.device, imgname+"_rn.txt");
            File f = new File(p.toAbsolutePath().toString());
            f.getParentFile().mkdirs();
            rn.noise.storeAsFloat(f);

            // Print Residual Noise infos
            System.out.println(rn.device + ": " + rn.imgName + ": " + rn.noise.toString());
        }

        // Comparing Reference Patterns with Residual Noises
        for (RPTuple rp : rps) {
            for (RNTuple rn : rns) {
                System.out.println("Comparing: " + rp.device + ":" + rn.device + " - " + rn.imgName);
                //Computing correlation between reference pattern and residual noise using CROP if necessary
                double v = SCIManager.compare(rp.pattern, rn.noise, AdaptationMethod.CROP);
                System.out.println("Result: " + v);
            }
        }

        // Comparing Reference Patterns with Reference Patterns
        for (RPTuple rp : rps) {
            for (RPTuple rp2 : rps) {
                System.out.println("Comparing: " + rp.device + ":" + rp2.device);
                //Computing correlation between reference pattern and residual noise using CROP if necessary
                double v = SCIManager.compare(rp.pattern, rp2.pattern, AdaptationMethod.CROP);
                System.out.println("Result: " + v);
            }
        }
    }
}
