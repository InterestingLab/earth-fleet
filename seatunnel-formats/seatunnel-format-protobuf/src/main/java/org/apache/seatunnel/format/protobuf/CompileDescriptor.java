package org.apache.seatunnel.format.protobuf;

import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.format.protobuf.exception.ProtobufFormatErrorCode;
import org.apache.seatunnel.format.protobuf.exception.SeaTunnelProtobufFormatException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class CompileDescriptor {

    public static  Descriptors.FileDescriptor[] compileDescriptorTempFile(String protoContent) throws IOException, InterruptedException, Descriptors.DescriptorValidationException {
        // Because Protobuf can only be dynamically parsed through the descriptor file, the file
        // needs to be compiled and generated. The following method is used here to solve the
        // problem: generate a temporary directory and compile .proto into a descriptor temporary
        // file. The temporary file and directory are deleted after the JVM runs.
        File tmpDir = File.createTempFile("tmp_protobuf_", "_proto");
        tmpDir.delete();
        tmpDir.mkdirs();
        tmpDir.deleteOnExit();
        File protoFile = new File(tmpDir, ".proto");
        protoFile.deleteOnExit();
        FileUtils.writeStringToFile(protoFile.getPath(), protoContent);
        String targetDesc = tmpDir + "/.desc";
        new File(targetDesc).deleteOnExit();

        int exitCode =
                Protoc.runProtoc(
                        new String[] {
                                "--proto_path=" + protoFile.getParent(),
                                "--descriptor_set_out=" + targetDesc,
                                protoFile.getPath()
                        });

        if (exitCode != 0) {
            throw new SeaTunnelProtobufFormatException(
                    ProtobufFormatErrorCode.DESCRIPTOR_CONVERT_FAILED,
                    "Protoc compile error, exit code: " + exitCode);
        }

        FileInputStream fis = new FileInputStream(targetDesc);
        DescriptorProtos.FileDescriptorSet descriptorSet =
                DescriptorProtos.FileDescriptorSet.parseFrom(fis);

        List<DescriptorProtos.FileDescriptorProto> fileDescriptors = descriptorSet.getFileList();
        Descriptors.FileDescriptor[] descriptorsArray =
                new Descriptors.FileDescriptor[fileDescriptors.size()];
        for (int i = 0; i < fileDescriptors.size(); i++) {
            descriptorsArray[i] =
                    Descriptors.FileDescriptor.buildFrom(
                            fileDescriptors.get(i), new Descriptors.FileDescriptor[] {});
        }
        return descriptorsArray;
    }
}
