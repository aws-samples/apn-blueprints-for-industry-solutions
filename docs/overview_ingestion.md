# Data Ingestion Pipeline Components

Data ingestion is the process of transporting data from one or more sources to a target storage medium for further processing and analysis.

Regardless of the industry, the initial stage of AWS customers' data journey is similar - data is gathered from multiple sources and funneled into the AWS Lake House. The `data ingestion pipeline` is the component which collects raw data from diverse sources and channel it into the AWS Lake House architecture. 


As data ingestion methods, AWS provides a variety of services and capabilities to ingest different types of data into your data lake house including [`Amazon AppFlow`]('https://aws.amazon.com/appflow/'), [`Kinesis Data Firehose`]('https://aws.amazon.com/kinesis/data-firehose/'), [`AWS Snow Family`]('https://aws.amazon.com/snow/'), [`Glue`]('https://aws.amazon.com/glue/'), [`DataSync`]('https://aws.amazon.com/datasync/'), [`AWS Transfer Family`]('https://aws.amazon.com/aws-transfer-family/'), [`Storage Gateway`]('https://aws.amazon.com/storagegateway/'), [`Direct Connect`]('https://aws.amazon.com/directconnect/'), [`Database Migration Service (DMS)`]('https://aws.amazon.com/dms/'), etc. 


But data ingestion can be a complex process due to various factors such as the type and source of the data, the method of ingestion (batch or real-time), data volume, and industry standards. The data may come from on-premises storage platforms like legacy data servers, mainframes, or data warehouses, as well as SaaS platforms, and may be structured or unstructured (e.g. images, text files, audio and video, and graphs). These factors require careful consideration during the design and implementation of data ingestion pipelines to ensure efficient and reliable transfer of data to the AWS Lake House.

The `Data Ingestion Pipeline Components` section within `AWS Industry Blueprints for Data & AI` offers a curated collection of ready-to-deploy modules including AWS ISV partner products, providing a head start in building robust data ingestion pipelines. These components are designed to streamline the process and accelerate development, enabling organizations to quickly establish efficient data pipelines. Furthermore, some of these components are tailored to meet industry-specific standards, ensuring compliance and alignment with regulations and requirements specific to sectors such as healthcare, finance, and more. With these deployment-ready modules, developers can expedite the creation of reliable data ingestion pipelines, saving time and effort in the solution development journey.



