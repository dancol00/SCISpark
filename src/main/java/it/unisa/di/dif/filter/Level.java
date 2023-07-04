package it.unisa.di.dif.filter;

public class Level implements WaveLevel{
    //	METODI NECESSARI PER WAVELET E FILTRO
    public void makeMatrixsCoef(int w, int h)
    {
        cA = new float[h][w];
        cH = new float[h][w];
        cV = new float[h][w];
        cD = new float[h][w];
    }

    public float[][] getcA() {
        return cA;
    }


    public void setcA(float[][] cA) {
        this.cA = cA;
    }


    public float[][] getcH() {
        return cH;
    }


    public void setcH(float[][] cH) {
        this.cH = cH;
    }


    public float[][] getcV() {
        return cV;
    }


    public void setcV(float[][] cV) {
        this.cV = cV;
    }


    public float[][] getcD() {
        return cD;
    }


    public void setcD(float[][] cD) {
        this.cD = cD;
    }


    //	METODI NECESSARI SOLO PER IL FILTRO
    public void setRigheCoef(int h)
    {
        height = h;
    }


    public void setColonneCoef(int w)
    {
        width = w;
    }


    public int getRighe(){
        return height;
    }


    public int getColonne(){
        return width;
    }


    public void setAddRow(){
        addRowCol[0] = true;
    }


    public void setAddCol(){
        addRowCol[1] = true;
    }


    public boolean[] getAddRowCol(){
        return addRowCol;
    }

    private int width;
    private int height;
    private boolean[] addRowCol = {false, false};
    private float[][] cA = null;
    private float[][] cH = null;
    private float[][] cV = null;
    private float[][] cD = null;
}
