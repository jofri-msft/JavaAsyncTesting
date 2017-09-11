public abstract class CloudBlob {

    /**
     * Holds the metadata for the blob.
     */
    HashMap<String, String> metadata = new HashMap<String, String>();

    /**
     * Holds the properties of the blob.
     */
    BlobProperties properties;

    /**
     * Holds the list of URIs for all locations.
     */
    private StorageUri storageUri;

    /**
     * Holds the snapshot ID.
     */
    String snapshotID;

    /**
     * Holds the blob's container reference.
     */
    private CloudBlobContainer container;

    /**
     * Represents the blob's directory.
     */
    //protected CloudBlobDirectory parent;

    /**
     * Holds the blob's name.
     */
    private String name;

    /**
     * Holds the number of bytes to buffer when writing to a {@link BlobOutputStream} (block and page blobs).
     */
    protected int streamWriteSizeInBytes = Constants.DEFAULT_STREAM_WRITE_IN_BYTES;

    /**
     * Holds the minimum read size when using a {@link BlobInputStream}.
     */
    protected int streamMinimumReadSizeInBytes = Constants.DEFAULT_MINIMUM_READ_SIZE_IN_BYTES;

}
