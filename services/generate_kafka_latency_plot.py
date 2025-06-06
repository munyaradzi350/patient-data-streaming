# generate_kafka_latency_plot.py

import matplotlib.pyplot as plt

# Sample manually extracted latencies (ms)
latency_data = {
    "PAT-00377": 85,
    "PAT-00378": 92,
    "PAT-00379": 89,
    "PAT-00380": 84,
    "PAT-00381": 86,
}

# Plot
plt.figure(figsize=(10, 6))
plt.bar(latency_data.keys(), latency_data.values(), color="skyblue")
plt.axhline(y=100, color="red", linestyle="--", label="Threshold (100 ms)")
plt.title("Kafka Streaming Latency per Patient")
plt.xlabel("Patient ID")
plt.ylabel("Latency (ms)")
plt.ylim(0, 120)
plt.legend()
plt.tight_layout()

# Save
plt.savefig("../figures/kafka_latency_metrics.png")
plt.show()
