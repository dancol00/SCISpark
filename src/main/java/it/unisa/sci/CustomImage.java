package it.unisa.sci;

import it.unisa.di.dif.pattern.Image;

import java.io.InputStream;

public class CustomImage extends Image {
    //private String cameraName;
    private String fileName;

    public CustomImage(InputStream is, String cameraName, String fileName) throws java.io.IOException{
        super(is);
    //    this.cameraName = cameraName;
        this.fileName = fileName;
    }

    // public String getCameraName() {
    //     return cameraName;
    // }

    public String getFileName() {
        return fileName;
    }
}
