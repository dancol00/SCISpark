package it.unisa.di.dif.filter;

public class Weiner {
    public WaveCoefficients weinerFilter(WaveCoefficients coef)
    {
        int i = 3;
        while(i >= 0)
        {
            WaveLevel level = coef.getLevel(i);
            float[][] cH = level.getcH();
            float[][] cV = level.getcV();
            float[][] cD = level.getcD();

            level.setcH(filter(cH));
            level.setcV(filter(cV));
            level.setcD(filter(cD));

            coef.setLevel(level, i);
            i--;
        }

        return coef;
    }

    public float[][] filter(float[][] matrix)
    {
        dimH = matrix.length;
        dimW = matrix[0].length;
        nDimH = dimH+8;
        nDimW = dimW+8;
        float[][] retMatrix = new float[dimH][dimW];
        float[][] newMatrix = new float[nDimH][nDimW];
        float[] sum = {0, 0, 0, 0};
        float[] varianza= new float[4];
        newMatrix = addForFilter(matrix);
        for(int i=4;i<dimH+4;i++)
        {
            for(int j=4;j<dimW+4;j++)
            {
                sum[0]=	(newMatrix[i-1][j-1])+(newMatrix[i-1][j])+(newMatrix[i-1][j+1])+
                        (newMatrix[i][j-1])+(newMatrix[i][j])+(newMatrix[i][j+1])+
                        (newMatrix[i+1][j-1])+(newMatrix[i+1][j])+(newMatrix[i+1][j+1]);

                varianza[0]=(sum[0]/9)-SIGMA;

                if(varianza[0]<0)
                    varianza[0]=0;

                sum[1]=	(newMatrix[i-2][j-2])+(newMatrix[i-2][j-1])+(newMatrix[i-2][j])+(newMatrix[i-2][j+1])+(newMatrix[i-2][j+2])+
                        (newMatrix[i-1][j-2])+(newMatrix[i-1][j-1])+(newMatrix[i-1][j])+(newMatrix[i-1][j+1])+(newMatrix[i-1][j+2])+
                        (newMatrix[i][j-2])+(newMatrix[i][j-1])+(newMatrix[i][j])+(newMatrix[i][j+1])+(newMatrix[i][j+2])+
                        (newMatrix[i+1][j-2])+(newMatrix[i+1][j-1])+(newMatrix[i+1][j])+(newMatrix[i+1][j+1])+(newMatrix[i+1][j+2])+
                        (newMatrix[i+2][j-2])+(newMatrix[i+2][j-1])+(newMatrix[i+2][j])+(newMatrix[i+2][j+1])+(newMatrix[i+2][j+2]);

                varianza[1]=(sum[1]/25)-SIGMA;

                if(varianza[1]<0)
                    varianza[1]=0;

                sum[2]=	(newMatrix[i-3][j-3])+(newMatrix[i-3][j-2])+(newMatrix[i-3][j-1])+(newMatrix[i-3][j])+(newMatrix[i-3][j+1])+(newMatrix[i-3][j+2])+(newMatrix[i-3][j+3])+
                        (newMatrix[i-2][j-3])+(newMatrix[i-2][j-2])+(newMatrix[i-2][j-1])+(newMatrix[i-2][j])+(newMatrix[i-2][j+1])+(newMatrix[i-2][j+2])+(newMatrix[i-2][j+3])+
                        (newMatrix[i-1][j-3])+(newMatrix[i-1][j-2])+(newMatrix[i-1][j-1])+(newMatrix[i-1][j])+(newMatrix[i-1][j+1])+(newMatrix[i-1][j+2])+(newMatrix[i-1][j+3])+
                        (newMatrix[i][j-3])+(newMatrix[i][j-2])+(newMatrix[i][j-1])+(newMatrix[i][j])+(newMatrix[i][j+1])+(newMatrix[i][j+2])+(newMatrix[i][j+3])+
                        (newMatrix[i+1][j-3])+(newMatrix[i+1][j-2])+(newMatrix[i+1][j-1])+(newMatrix[i+1][j])+(newMatrix[i+1][j+1])+(newMatrix[i+1][j+2])+(newMatrix[i+1][j+3])+
                        (newMatrix[i+2][j-3])+(newMatrix[i+2][j-2])+(newMatrix[i+2][j-1])+(newMatrix[i+2][j])+(newMatrix[i+2][j+1])+(newMatrix[i+2][j+2])+(newMatrix[i+2][j+3])+
                        (newMatrix[i+3][j-3])+(newMatrix[i+3][j-2])+(newMatrix[i+3][j-1])+(newMatrix[i+3][j])+(newMatrix[i+3][j+1])+(newMatrix[i+3][j+2])+(newMatrix[i+3][j+3]);

                varianza[2]=(sum[2]/49)-SIGMA;

                if(varianza[2]<0)
                    varianza[2]=0;

                sum[3]=	(newMatrix[i-4][j-4])+(newMatrix[i-4][j-3])+(newMatrix[i-4][j-2])+(newMatrix[i-4][j-1])+(newMatrix[i-4][j])+(newMatrix[i-4][j+1])+(newMatrix[i-4][j+2])+(newMatrix[i-4][j+3])+(newMatrix[i-4][j+4])+
                        (newMatrix[i-3][j-4])+(newMatrix[i-3][j-3])+(newMatrix[i-3][j-2])+(newMatrix[i-3][j-1])+(newMatrix[i-3][j])+(newMatrix[i-3][j+1])+(newMatrix[i-3][j+2])+(newMatrix[i-3][j+3])+(newMatrix[i-3][j+4])+
                        (newMatrix[i-2][j-4])+(newMatrix[i-2][j-3])+(newMatrix[i-2][j-2])+(newMatrix[i-2][j-1])+(newMatrix[i-2][j])+(newMatrix[i-2][j+1])+(newMatrix[i-2][j+2])+(newMatrix[i-2][j+3])+(newMatrix[i-2][j+4])+
                        (newMatrix[i-1][j-4])+(newMatrix[i-1][j-3])+(newMatrix[i-1][j-2])+(newMatrix[i-1][j-1])+(newMatrix[i-1][j])+(newMatrix[i-1][j+1])+(newMatrix[i-1][j+2])+(newMatrix[i-1][j+3])+(newMatrix[i-1][j+4])+
                        (newMatrix[i][j-4])+(newMatrix[i][j-3])+(newMatrix[i][j-2])+(newMatrix[i][j-1])+(newMatrix[i][j])+(newMatrix[i][j+1])+(newMatrix[i][j+2])+(newMatrix[i][j+3])+(newMatrix[i][j+4])+
                        (newMatrix[i+1][j-4])+(newMatrix[i+1][j-3])+(newMatrix[i+1][j-2])+(newMatrix[i+1][j-1])+(newMatrix[i+1][j])+(newMatrix[i+1][j+1])+(newMatrix[i+1][j+2])+(newMatrix[i+1][j+3])+(newMatrix[i+1][j+4])+
                        (newMatrix[i+2][j-4])+(newMatrix[i+2][j-3])+(newMatrix[i+2][j-2])+(newMatrix[i+2][j-1])+(newMatrix[i+2][j])+(newMatrix[i+2][j+1])+(newMatrix[i+2][j+2])+(newMatrix[i+2][j+3])+(newMatrix[i+2][j+4])+
                        (newMatrix[i+3][j-4])+(newMatrix[i+3][j-3])+(newMatrix[i+3][j-2])+(newMatrix[i+3][j-1])+(newMatrix[i+3][j])+(newMatrix[i+3][j+1])+(newMatrix[i+3][j+2])+(newMatrix[i+3][j+3])+(newMatrix[i+3][j+4])+
                        (newMatrix[i+4][j-4])+(newMatrix[i+4][j-3])+(newMatrix[i+4][j-2])+(newMatrix[i+4][j-1])+(newMatrix[i+4][j])+(newMatrix[i+4][j+1])+(newMatrix[i+4][j+2])+(newMatrix[i+4][j+3])+(newMatrix[i+4][j+4]);

                varianza[3]=(sum[3]/81)-SIGMA;

                if(varianza[3]<0)
                    varianza[3]=0;

                float minvar=min(varianza);
                retMatrix[i-4][j-4]=matrix[i-4][j-4]*(minvar/(minvar+25));
            }
        }

        return retMatrix;
    }

    public float[][] addForFilter(float[][] matrix)
    {
        float[][] newFiltMatrix = new float[nDimH][nDimW];

        for(int i=4;i<nDimW-4;i++)
        {
            newFiltMatrix[0][i]=matrix[4][i-4]*matrix[4][i-4];
            newFiltMatrix[1][i]=matrix[3][i-4]*matrix[3][i-4];
            newFiltMatrix[2][i]=matrix[2][i-4]*matrix[2][i-4];
            newFiltMatrix[3][i]=matrix[1][i-4]*matrix[1][i-4];

            newFiltMatrix[nDimH-1][i]=matrix[dimH-4][i-4]*matrix[dimH-4][i-4];
            newFiltMatrix[nDimH-2][i]=matrix[dimH-3][i-4]*matrix[dimH-3][i-4];
            newFiltMatrix[nDimH-3][i]=matrix[dimH-2][i-4]*matrix[dimH-2][i-4];
            newFiltMatrix[nDimH-4][i]=matrix[dimH-1][i-4]*matrix[dimH-1][i-4];
        }

        for(int i=0;i<dimH;i++)
        {
            for(int j=0;j<dimW;j++)
                newFiltMatrix[i+4][j+4]=matrix[i][j]*matrix[i][j];
        }
        for(int i=0;i<nDimH;i++)
        {
            newFiltMatrix[i][0]=newFiltMatrix[i][8]*newFiltMatrix[i][8];
            newFiltMatrix[i][1]=newFiltMatrix[i][7]*newFiltMatrix[i][7];
            newFiltMatrix[i][2]=newFiltMatrix[i][6]*newFiltMatrix[i][6];
            newFiltMatrix[i][3]=newFiltMatrix[i][5]*newFiltMatrix[i][5];

            newFiltMatrix[i][nDimW-1]=newFiltMatrix[i][nDimW-9]*newFiltMatrix[i][nDimW-9];
            newFiltMatrix[i][nDimW-2]=newFiltMatrix[i][nDimW-8]*newFiltMatrix[i][nDimW-8];
            newFiltMatrix[i][nDimW-3]=newFiltMatrix[i][nDimW-7]*newFiltMatrix[i][nDimW-7];
            newFiltMatrix[i][nDimW-4]=newFiltMatrix[i][nDimW-6]*newFiltMatrix[i][nDimW-6];

        }

        return newFiltMatrix;
    }

    private float min(float[] x)
    {
        for (int i=0; i<x.length-1; i++)
        {
            int minIndex = i;
            for (int j=i+1; j<x.length; j++)
            {
                if (x[minIndex] > x[j])
                {
                    minIndex = j;
                }
            }
            if (minIndex != i)
            {
                float temp = x[i];
                x[i] = x[minIndex];
                x[minIndex] = temp;
            }
        }
        return x[0];
    }

    int nDimH;
    int nDimW;
    int dimH;
    int livello;
    int dimW;
    private final static int SIGMA = 25;
}
