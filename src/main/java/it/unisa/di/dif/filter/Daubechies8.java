package it.unisa.di.dif.filter;

public class Daubechies8 {
    public void setMatrixWork(float[][] matWork)
    {
        matrix = matWork;
        matrixInv = null;
    }


    public Level dec2D()
    {
        int dimW = matrix[0].length/2;
        int dimH = matrix.length/2;

        mraLevel = new Level();
        mraLevel.makeMatrixsCoef(dimW,dimH);
        mraLevel.setRigheCoef(dimH);
        mraLevel.setColonneCoef(dimW);
        coefA = new float[dimH][dimW];
        coefH = new float[dimH][dimW];
        coefV = new float[dimH][dimW];
        coefD = new float[dimH][dimW];

        decRow();
        decCol();

        mraLevel.setcA(coefA);
        mraLevel.setcH(coefH);
        mraLevel.setcV(coefV);
        mraLevel.setcD(coefD);

        return mraLevel;
    }


    public float[][] invDec2D(WaveCoefficients coeff, int level)
    {
        invDecCol(coeff, level);
        invDecRow();
        return matrixInv;
    }


    private void decRow()
    {
        int dimH = matrix.length;
        int dimW = matrix[0].length;

        float[] vetRig = new float[dimW];

        for(int z=0; z<dimH; z++)
        {
            for(int j=0; j<dimW; j++)
                vetRig[j] = matrix[z][j];

            dwt(vetRig, dimW, z, false);
        }
    }


    private void invDecRow()
    {
        int dimH = matrixInv.length;
        int dimW = matrixInv[0].length;

        float[] vetRig = new float[dimW];
        float[] ris = null;

        for(int z=0; z<dimH; z++)
        {
            for(int j=0; j<dimW; j++)
                vetRig[j] = matrixInv[z][j];

            ris = inverseDwt(vetRig, dimW);

            for(int k=0; k<dimW; k++)
                matrixInv[z][k] = (float)ris[k];
        }
    }


    private void decCol()
    {
        int dimH = matrix.length;
        int dimW = matrix[0].length;

        float[] vetCol = new float[dimH];

        for(int z=0; z<dimW; z++)
        {
            if(z < coefA[0].length)
            {
                for(int i=0; i<coefA.length; i++)
                    vetCol[i] = coefA[i][z];

                for(int i=coefA.length; i<dimH; i++)
                    vetCol[i] = coefV[i%coefV.length][z];
            }

            else
            {
                for(int i=0; i<coefH.length; i++)
                    vetCol[i] = coefH[i][z%coefH[0].length];

                for(int i=coefH.length; i<dimH; i++)
                    vetCol[i] = coefD[i%coefD.length][z%coefD[0].length];
            }

            dwt(vetCol, dimH, z, true);
        }
    }


    private void invDecCol(WaveCoefficients coeff, int level)
    {
        WaveLevel lvMRA = coeff.getLevel(level);
        float[][] cA = lvMRA.getcA();
        float[][] cH = lvMRA.getcH();
        float[][] cV = lvMRA.getcV();
        float[][] cD = lvMRA.getcD();
        int	dimH = cA.length + cV.length;
        int	dimW = cA[0].length + cH[0].length;
        matrixInv = new float[dimH][dimW];

        float[] vetCol = new float[dimH];
        float[] ris = null;

        for(int z=0; z<dimW; z++)
        {
            if(z < cA[0].length)
            {
                for(int i=0; i<cA.length; i++)
                    vetCol[i] = cA[i][z];

                for(int i=cA.length; i<dimH; i++)
                    vetCol[i] = cV[i%cV.length][z];
            }

            else
            {
                for(int i=0; i<cH.length; i++)
                    vetCol[i] = cH[i][z%cH[0].length];

                for(int i=cH.length; i<dimH; i++)
                    vetCol[i] = cD[i%cD.length][z%cD[0].length];
            }

            ris = inverseDwt(vetCol, dimH);

            for(int k=0; k<dimH; k++)
                matrixInv[k][z] = (float)ris[k];
        }
    }


    private void fillCoefAVH(int row, int col, double val, boolean dwtCol)
    {
        if(!dwtCol)
        {
            if(row < (matrix.length/2))
                coefA[row][col] = (float)val;
            else
                coefV[row%(matrix.length/2)][col] = (float)val;
        }

        else
        {
            if(row < (matrix[0].length/2))
                coefA[col][row] = (float)val;
            else
                coefH[col][row%(matrix[0].length/2)] = (float)val;
        }
    }

    private void fillCoefHDV(int row, int col, double val, boolean dwtCol)
    {
        if(!dwtCol)
        {
            if(row < (matrix.length/2))
                coefH[row][col] = (float)val;
            else
                coefD[row%(matrix.length/2)][col] = (float)val;
        }

        else
        {
            if(row < (matrix[0].length/2))
                coefV[col][row] = (float)val;
            else
                coefD[col][row%(matrix[0].length/2)] = (float)val;
        }
    }


    private void dwt(float[] vetMat, int dim, int row, boolean dwtCol)
    {
//		double[] ris = new double[dim];
        float ris = 0;

//		Firsts approximation coefficients
        ris=(float)(h[0]*vetMat[0]+h[1]*vetMat[1]+h[2]*vetMat[2]+
                h[3]*vetMat[3]+h[4]*vetMat[4]+h[5]*vetMat[5]+
                h[6]*vetMat[6]+h[7]*vetMat[7]);

        fillCoefAVH(row, 0, ris, dwtCol);

        ris=(float)(g[0]*vetMat[0]+g[1]*vetMat[1]+g[2]*vetMat[2]+
                g[3]*vetMat[3]+g[4]*vetMat[4]+g[5]*vetMat[5]+
                g[6]*vetMat[6]+g[7]*vetMat[7]);

        fillCoefHDV(row, 0, ris, dwtCol);

//		Coefficients with zero to left and right
        int j=1;
        for(int i=2; i<dim-6; i+=2, j++)
        {
            ris=(float)(h[0]*vetMat[i]+h[1]*vetMat[i+1]+h[2]*vetMat[i+2]+
                    h[3]*vetMat[i+3]+h[4]*vetMat[i+4]+h[5]*vetMat[i+5]+
                    h[6]*vetMat[i+6]+h[7]*vetMat[i+7]);

            fillCoefAVH(row, j, ris, dwtCol);

            ris=(float)(g[0]*vetMat[i]+g[1]*vetMat[i+1]+g[2]*vetMat[i+2]+
                    g[3]*vetMat[i+3]+g[4]*vetMat[i+4]+g[5]*vetMat[i+5]+
                    g[6]*vetMat[i+6]+g[7]*vetMat[i+7]);

            fillCoefHDV(row, j, ris, dwtCol);

        }

//		Coefficients in triangle diagram
        for(int k=j; k<dim/2; k++)
        {
            ris = (float)(h[0]*vetMat[(k*2)%dim]+h[1]*vetMat[(k*2+1)%dim]+h[2]*vetMat[(k*2+2)%dim]+
                    h[3]*vetMat[(k*2+3)%dim]+h[4]*vetMat[(k*2+4)%dim]+h[5]*vetMat[(k*2+5)%dim]+
                    h[6]*vetMat[(k*2+6)%dim]+h[7]*vetMat[(k*2+7)%dim]);

            fillCoefAVH(row, k, ris, dwtCol);

            ris = (float)(g[0]*vetMat[(k*2)%dim]+g[1]*vetMat[(k*2+1)%dim]+g[2]*vetMat[(k*2+2)%dim]+
                    g[3]*vetMat[(k*2+3)%dim]+g[4]*vetMat[(k*2+4)%dim]+g[5]*vetMat[(k*2+5)%dim]+
                    g[6]*vetMat[(k*2+6)%dim]+g[7]*vetMat[(k*2+7)%dim]);

            fillCoefHDV(row, k, ris, dwtCol);
        }
    }


    private float[] inverseDwt(float[] vetMat, int dim)
    {
        float[] ris = new float[dim];
        int j = 0;

        ris[j++]=(float)(h[0]*vetMat[0]+g[0]*vetMat[dim/2]+
                h[2]*vetMat[(dim/2)-1]+g[2]*vetMat[dim-1]+
                h[4]*vetMat[(dim/2)-2]+g[4]*vetMat[dim-2]+
                h[6]*vetMat[(dim/2)-3]+g[6]*vetMat[dim-3]);

        ris[j++]=(float)(h[1]*vetMat[0]+g[1]*vetMat[dim/2]+
                h[3]*vetMat[(dim/2)-1]+g[3]*vetMat[dim-1]+
                h[5]*vetMat[(dim/2)-2]+g[5]*vetMat[dim-2]+
                h[7]*vetMat[(dim/2)-3]+g[7]*vetMat[dim-3]);

        ris[j++]=(float)(h[2]*vetMat[0]+g[2]*vetMat[dim/2]+
                h[0]*vetMat[1]+g[0]*vetMat[(dim/2)+1]+
                h[4]*vetMat[(dim/2)-1]+g[4]*vetMat[dim-1]+
                h[6]*vetMat[(dim/2)-2]+g[6]*vetMat[dim-2]);

        ris[j++]=(float)(h[3]*vetMat[0]+g[3]*vetMat[dim/2]+
                h[1]*vetMat[1]+g[1]*vetMat[(dim/2)+1]+
                h[5]*vetMat[(dim/2)-1]+g[5]*vetMat[dim-1]+
                h[7]*vetMat[(dim/2)-2]+g[7]*vetMat[dim-2]);

        ris[j++]=(float)(h[4]*vetMat[0]+g[4]*vetMat[dim/2]+
                h[2]*vetMat[1]+g[2]*vetMat[(dim/2)+1]+
                h[0]*vetMat[2]+g[0]*vetMat[(dim/2)+2]+
                h[6]*vetMat[(dim/2)-1]+g[6]*vetMat[dim-1]);

        ris[j++]=(float)(h[5]*vetMat[0]+g[5]*vetMat[dim/2]+
                h[3]*vetMat[1]+g[3]*vetMat[(dim/2)+1]+
                h[1]*vetMat[2]+g[1]*vetMat[(dim/2)+2]+
                h[7]*vetMat[(dim/2)-1]+g[7]*vetMat[dim-1]);

        for(int i=0;i<(dim/2)-3;i++)
        {
            ris[j++]=(float)(h[6]*vetMat[i]+g[6]*vetMat[i+(dim/2)]+
                    h[4]*vetMat[i+1]+g[4]*vetMat[i+(dim/2)+1]+
                    h[2]*vetMat[i+2]+g[2]*vetMat[i+(dim/2)+2]+
                    h[0]*vetMat[i+3]+g[0]*vetMat[i+(dim/2)+3]);

            ris[j++]=(float)(h[7]*vetMat[i]+g[7]*vetMat[i+(dim/2)]+
                    h[5]*vetMat[i+1]+g[5]*vetMat[i+(dim/2)+1]+
                    h[3]*vetMat[i+2]+g[3]*vetMat[i+(dim/2)+2]+
                    h[1]*vetMat[i+3]+g[1]*vetMat[i+(dim/2)+3]);
        }

        return ris;
    }


    private Level mraLevel = null;
    private float[][] coefA = null;
    private float[][] coefH = null;
    private float[][] coefV = null;
    private float[][] coefD = null;

    private float[][] matrix = null;
    private float[][] matrixInv = null;

    public static double[] h = {
            2.303778133088965008632911830440708500016152482483092977910968e-01,
            7.148465705529156470899219552739926037076084010993081758450110e-01,
            6.308807679298589078817163383006152202032229226771951174057473e-01,
            -2.798376941685985421141374718007538541198732022449175284003358e-02,
            -1.870348117190930840795706727890814195845441743745800912057770e-01,
            3.084138183556076362721936253495905017031482172003403341821219e-02,
            3.288301166688519973540751354924438866454194113754971259727278e-02,
            -1.059740178506903210488320852402722918109996490637641983484974e-02
    };

    public static double[] g = {h[7], -h[6], h[5], -h[4], h[3], -h[2], h[1], -h[0]};
}
