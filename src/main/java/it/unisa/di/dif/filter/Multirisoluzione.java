package it.unisa.di.dif.filter;

import it.unisa.di.dif.pattern.ColorChannel;

public class Multirisoluzione implements Filter
{
    public Multirisoluzione()
    {
        daubechies = new Daubechies8();
    }


    public float[][] matrixControl(float[][] matrix)
    {
        width = matrix[0].length;
        height = matrix.length;
        int nHeight;
        int nWidth;
        double testWidth = width%2;
        double testHeight = height%2;
        float[][] newFiltMatrix;

        if(testHeight==0)
            nHeight=height+16;
        else
            nHeight=height+15;

        if(testWidth==0)
            nWidth=width+16;
        else
            nWidth=width+15;

        newFiltMatrix=new float[nHeight][nWidth];

        for(int i=8;i<nWidth-8;i++)
        {
            newFiltMatrix[0][i]=matrix[8][i-8];
            newFiltMatrix[1][i]=matrix[7][i-8];
            newFiltMatrix[2][i]=matrix[6][i-8];
            newFiltMatrix[3][i]=matrix[5][i-8];
            newFiltMatrix[4][i]=matrix[4][i-8];
            newFiltMatrix[5][i]=matrix[3][i-8];
            newFiltMatrix[6][i]=matrix[2][i-8];
            newFiltMatrix[7][i]=matrix[1][i-8];

            newFiltMatrix[nHeight-1][i]=matrix[height-8][i-8];
            newFiltMatrix[nHeight-2][i]=matrix[height-7][i-8];
            newFiltMatrix[nHeight-3][i]=matrix[height-6][i-8];
            newFiltMatrix[nHeight-4][i]=matrix[height-5][i-8];
            newFiltMatrix[nHeight-5][i]=matrix[height-4][i-8];
            newFiltMatrix[nHeight-6][i]=matrix[height-3][i-8];
            newFiltMatrix[nHeight-7][i]=matrix[height-2][i-8];
            if(testHeight==0)
                newFiltMatrix[nHeight-8][i]=matrix[height-1][i-8];
        }

        for(int i=0;i<height;i++)
        {
            for(int j=0;j<width;j++)
                newFiltMatrix[i+8][j+8]=matrix[i][j];
        }
//		System.out.println(nDimH+"------"+nDimW+" "+dimH+"------"+dimW);
        for(int i=0;i<nHeight;i++)
        {
            newFiltMatrix[i][0]=newFiltMatrix[i][9];
            newFiltMatrix[i][1]=newFiltMatrix[i][10];
            newFiltMatrix[i][2]=newFiltMatrix[i][11];
            newFiltMatrix[i][3]=newFiltMatrix[i][12];
            newFiltMatrix[i][4]=newFiltMatrix[i][13];
            newFiltMatrix[i][5]=newFiltMatrix[i][14];
            newFiltMatrix[i][6]=newFiltMatrix[i][15];
            newFiltMatrix[i][7]=newFiltMatrix[i][16];

            newFiltMatrix[i][nWidth-1]=newFiltMatrix[i][nWidth-9];
            newFiltMatrix[i][nWidth-2]=newFiltMatrix[i][nWidth-10];
            newFiltMatrix[i][nWidth-3]=newFiltMatrix[i][nWidth-11];
            newFiltMatrix[i][nWidth-4]=newFiltMatrix[i][nWidth-12];
            newFiltMatrix[i][nWidth-5]=newFiltMatrix[i][nWidth-13];
            newFiltMatrix[i][nWidth-6]=newFiltMatrix[i][nWidth-14];
            newFiltMatrix[i][nWidth-7]=newFiltMatrix[i][nWidth-15];
            if(testWidth==0)
                newFiltMatrix[i][nWidth-8]=newFiltMatrix[i][nWidth-16];

        }
//		if(testWidth != 0 || testHeight != 0)
//		{
//			int newWidth = width;
//			int newHeight = height;
//
//			if(testWidth != 0)
//				newWidth = width + 1;
//
//			if(testHeight != 0)
//				newHeight = height +1;
//
//			double[][] zeroMatrix = addZero(matrix, newWidth, newHeight);
//			return zeroMatrix;
//		}

//		return matrix;
        return newFiltMatrix;
    }

    public ColorChannel getFiltered(ColorChannel canale)
    {
        float[][] c = canale.getData();
        Weiner wein=new Weiner();
        wave_coeff=waveTransform(c,4);
        WaveCoefficients filt_wave_coeff=wein.weinerFilter(wave_coeff);
        return new ColorChannel(invTransform(filt_wave_coeff), canale.getChannel());
    }

    public String getInfo()
    {
        return "Filtro PNU (Febbraio 2010) [Vincenzo De Maio, Antonio Iovino]";
    }

    public Object useWavelet()
    {
        return wave_coeff;
    }

    public WaveCoefficients waveTransform(float[][] matrix, int level)
    {
        int i = 0;
        this.level = level;
        float[][] matWork = null;
        coeff = new Coefficients();
        coeff.makeVectWL(level);

//		Multi resolution until level LEVEL
        while(i < level)
        {
            matWork = matrixControl(matrix);
            daubechies.setMatrixWork(matWork);
            Level mraLevel = daubechies.dec2D();

//			if(matWork[0].length != width)
            if(matrix[0].length%2==1)
                mraLevel.setAddCol();

//			if(matWork.length != height)
            if(matrix.length%2==1)
                mraLevel.setAddRow();

            coeff.setLevel(mraLevel, i);
            matrix = mraLevel.getcA();
            i++;
        }

        return coeff;
    }


    public float[][] invTransform(WaveCoefficients coef)
    {
        int i = level;
        float[][] newMatrix = null;

//		Inverse multi resolution
        while(i > 1)
        {
            newMatrix = daubechies.invDec2D(coeff, i-1);
            WaveLevel actLevel = coeff.getLevel(i-1);
            boolean[] addRowCol = ((Level)actLevel).getAddRowCol();
            WaveLevel backLevel = coeff.getLevel(i-2);

//			if(addRowCol[0] || addRowCol[1])
//				backLevel.setcA(removeZero(newMatrix, addRowCol));
//			else
//				backLevel.setcA(newMatrix);
            backLevel.setcA(removeEst(newMatrix, addRowCol));
            i--;
        }

        newMatrix = daubechies.invDec2D(coeff, i-1);
        WaveLevel actLevel = coeff.getLevel(i-1);
        boolean[] addRowCol = ((Level)actLevel).getAddRowCol();

//		if(addRowCol[0] || addRowCol[1])
//			newMatrix = removeZero(newMatrix, addRowCol);

        newMatrix = removeEst(newMatrix, addRowCol);

        return newMatrix;
    }

    public float[][] removeEst(float[][] zeroMatrix, boolean[] flag)
    {
        int realH = zeroMatrix.length;
        int realW = zeroMatrix[0].length;
        float[][] realMatrix;
        int untilCol;
        int untilRow;

        if(flag[0])
            untilRow=realH-7;
        else
            untilRow=realH-8;

        if(flag[1])
            untilCol=realW-7;
        else
            untilCol=realW-8;

        realMatrix=new float[untilRow-8][untilCol-8];

        for(int i=0;i<untilRow-8;i++)
            for(int j=0;j<untilCol-8;j++)
                realMatrix[i][j]=zeroMatrix[i+8][j+8];

        return realMatrix;
    }

    public float[][] addZero(float[][] matrixOrig, int newWidth, int newHeight)
    {
        float[][] zeroMatrix = new float[newHeight][newWidth];

        for(int i=0; i<height; i++)
        {
            for(int j=0; j<width; j++)
                zeroMatrix[i][j] = matrixOrig[i][j];
        }

        for(int i=0; i<newHeight; i++)
        {
            for(int j=width; j<newWidth; j++)
                zeroMatrix[i][j] = 0;
        }

        for(int i=height; i<newHeight; i++)
        {
            for(int j=0; j<width; j++)
                zeroMatrix[i][j] = 0;
        }

        return zeroMatrix;
    }


    public float[][] removeZero(float[][] zeroMatrix, boolean[] flag)
    {
        int realH = zeroMatrix.length;
        int realW = zeroMatrix[0].length;

        if(flag[0])
            realH = realH - 1;

        if(flag[1])
            realW = realW - 1;

        float[][] matrixOrig = new float[realH][realW];
        for(int i=0; i<realH; i++)
        {
            for(int j=0; j<realW; j++)
                matrixOrig[i][j] = zeroMatrix[i][j];
        }

        return matrixOrig;
    }

    int width = 0;
    int height = 0;

    private Daubechies8 daubechies = null;
    private WaveCoefficients coeff = null;
    private int level = 0;
    private WaveCoefficients wave_coeff = null;
}