package bean;

public class Source {

    public enum Type {
        LIST, HASH
    }

    private Type type;
    private String ip;
    private int port;
    private String name;

    public Source() {
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setType(String type) throws LSException {
        type = type.toLowerCase();
        switch (type) {
            case "list":
                this.type = Type.LIST;
                break;
            case "hash":
                this.type = Type.HASH;
                break;
            default:
                throw new LSException("数据源[" + type + "]暂不支持");
        }
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
