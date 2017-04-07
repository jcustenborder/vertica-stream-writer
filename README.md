# Introduction

This library is a helper library to stream data in the [Vertica Native Binary Format](https://my.vertica.com/docs/8.0.x/HTML/index.htm#Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm).
The goal is to use a [VerticaCopyStream](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/ConnectingToHPVertica/ClientJDBC/UsingVerticaCopyStream.htm) to 
import data to Vertica in the most efficient way possible. 
 
# Example

Below is a direct example of building the example file defined in the Vertica Documentation [Creating Native Binary Format Files](https://my.vertica.com/docs/8.0.x/HTML/index.htm#Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm)

```java

    VerticaStreamWriterBuilder streamWriterBuilder = new VerticaStreamWriterBuilder()
        .table("allTypes")
        .column("INTCOL", VerticaType.INTEGER, 8)
        .column("FLOATCOL", VerticaType.FLOAT)
        .column("CHARCOL", VerticaType.CHAR, 10)
        .column("VARCHARCOL", VerticaType.VARCHAR)
        .column("BOOLCOL", VerticaType.BOOLEAN)
        .column("DATECOL", VerticaType.DATE)
        .column("TIMESTAMPCOL", VerticaType.TIMESTAMP)
        .column("TIMESTAMPTZCOL", VerticaType.TIMESTAMPTZ)
        .column("TIMECOL", VerticaType.TIME)
        .column("TIMETZCOL", VerticaType.TIMETZ)
        .column("VARBINCOL", VerticaType.VARBINARY)
        .column("BINCOL", VerticaType.BINARY, 3)
        .column("NUMCOL", VerticaType.NUMERIC, 38, 0)
        .column("INTERVALCOL", VerticaType.INTERVAL);

    Object[] row = new Object[]{
        1,
        -1.11D,
        "one       ",
        "ONE",
        true,
        new Date(915753600000L),
        new Date(919739512350L),
        date("yyyy-MM-dd HH:mm:ssX", "1999-01-08 07:04:37-05"),
        date("HH:mm:ss", "07:09:23"),
        date("HH:mm:ssX", "15:12:34-05"),
        BaseEncoding.base16().decode("ABCD"),
        BaseEncoding.base16().decode("ABCD"),
        BigDecimal.valueOf(1234532),
        (Duration.ofHours(3).plusMinutes(3).plusSeconds(3).toMillis() * 1000L)
    };

    assertEquals(14, streamWriterBuilder.columnInfos.size(), "column count should match.");

    final String actual;

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      try (VerticaStreamWriter streamWriter = streamWriterBuilder.build(outputStream)) {
        streamWriter.write(row);
      }
    }

```