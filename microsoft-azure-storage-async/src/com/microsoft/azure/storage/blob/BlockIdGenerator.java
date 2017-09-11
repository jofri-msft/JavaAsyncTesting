import com.google.common.io.BaseEncoding;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianghlu on 7/22/17.
 */
public class BlockIdGenerator {
    private long total;
    private long blockSize;
    private long blocks;
    private int blockIdLength;
    private List<String> blockIds;

    public BlockIdGenerator(long total, long blockSize) {
        this.total = total;
        this.blockSize = blockSize;
        this.blocks = total / blockSize + 1;
        this.blockIdLength = (int) Math.floor(Math.log10(blocks)) + 1;
        this.blockIds = new ArrayList<>();
    }

    public synchronized String getBlockId() {
        String blockId = String.format("%0" + blockIdLength + "d", blockIds.size());
        blockId = BaseEncoding.base64().encode(blockId.getBytes());
        blockIds.add(blockId);
        return blockId;
    }

    public String getBlockListXml() {
        StringBuilder builder = new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
        builder.append("<BlockList>");
        for (String id : blockIds) {
            builder.append("<Latest>").append(id).append("</Latest>");
        }
        builder.append("</BlockList>");
        return builder.toString();
    }
}
