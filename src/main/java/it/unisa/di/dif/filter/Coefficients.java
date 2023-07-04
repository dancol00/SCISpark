package it.unisa.di.dif.filter;

public class Coefficients implements WaveCoefficients
{
    public void makeVectWL(int numLevel)
    {
        mraLevel = new Level[numLevel];
    }

    public void setLevel(WaveLevel level, int num)
    {
        mraLevel[num] = level;
    }

    public WaveLevel getLevel(int num)
    {
        return mraLevel[num];
    }


    private WaveLevel[] mraLevel = null;
    private int numLevel = 0;
}
