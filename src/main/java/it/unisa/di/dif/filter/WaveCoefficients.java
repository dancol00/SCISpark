package it.unisa.di.dif.filter;

public interface WaveCoefficients {
    //	Crea un vettore di oggetti WaveLevel
    public void makeVectWL(int numLevel);

    //	Inserisce nel vettore di oggetti WaveLevel nella posizione nunm un oggetto di tipo WaveLevl
    public void setLevel(WaveLevel level, int num);

    //	Restituisce l'oggetto WaveLevel in posizione num nel vettore
    public WaveLevel getLevel(int num);
}
