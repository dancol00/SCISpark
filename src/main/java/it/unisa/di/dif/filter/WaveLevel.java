package it.unisa.di.dif.filter;

public interface WaveLevel {
    //	Crea le matrici dei coefficienti cA, cH, cV, cD
    public void makeMatrixsCoef(int w, int h);

    public float[][] getcA();

    public void setcA(float[][] cA);

    public float[][] getcH();

    public void setcH(float[][] cH);

    public float[][] getcV();

    public void setcV(float[][] cV);

    public float[][] getcD();

    public void setcD(float[][] cD);

    //	Setta il numero di righe delle matrici cA, cH, cV, cD
    public void setRigheCoef(int h);

    //	Setta il numero di colonne delle matrici cA, cH, cV, cD
    public void setColonneCoef(int w);

    public int getRighe();

    public int getColonne();
}
