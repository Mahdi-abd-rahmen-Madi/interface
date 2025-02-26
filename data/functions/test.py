import fiona

# List all supported drivers
print("Supported drivers:", fiona.supported_drivers)

# Check FlatGeobuf capabilities
if "FlatGeobuf" in fiona.supported_drivers:
    print("FlatGeobuf is supported.")
else:
    print("FlatGeobuf is NOT supported.")