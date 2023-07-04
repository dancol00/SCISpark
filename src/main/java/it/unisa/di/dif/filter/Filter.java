package it.unisa.di.dif.filter;

import it.unisa.di.dif.pattern.ColorChannel;

public interface Filter {
    public ColorChannel getFiltered(ColorChannel channel);

    public String getInfo();
}
