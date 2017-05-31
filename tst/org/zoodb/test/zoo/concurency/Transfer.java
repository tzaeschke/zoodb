package org.zoodb.test.zoo.concurency;

import org.zoodb.api.impl.ZooPC;

public class Transfer extends ZooPC {
    private String fromAccount;
    private String toAccount;
    private int amount;
    private boolean booked;

    public Transfer() { }

    public Transfer(String from, String to, int amount)
    {
        this.fromAccount = from;
        this.toAccount = to;
        this.amount = amount;
        this.booked = false;
    }

    public int getAmount()
    {
        return amount;
    }

    public boolean isBooked()
    {
        return booked;
    }

    public String getFromAccount()
    {
        return fromAccount;
    }

    public String getToAccount()
    {
        return toAccount;
    }

    public void setAmount(int i)
    {
        amount = i;
    }

    public void setBooked(boolean b)
    {
        booked = b;
    }

    public void setFromAccount(String string)
    {
        fromAccount = string;
    }

    public void setToAccount(String string)
    {
        toAccount = string;
    }

    @Override
    public String toString()
    {
        return "Transfer-" + super.toString() + " : " + amount +
            " from " + fromAccount + " to " + toAccount + (booked ? " is booked." : " is not yet booked.");
    }
}