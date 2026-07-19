using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using ClickHouse.Client.ADO;

namespace DataFlowStudio.Clickhouse;

/// <summary>
/// Creates ClickHouse connections for the lab, whose HTTPS interface presents a certificate signed
/// by the platform's private Vault-PKI root — which the host OS does not trust. Given a CA bundle
/// this validates the server chain against that private root (via a custom trust store) instead of
/// the OS store, and optionally presents a client certificate for mTLS. With no CA path it returns a
/// plain connection (e.g. for a local test container over HTTP).
/// </summary>
public static class TlsClickHouseConnectionFactory
{
    /// <summary>
    /// Builds a connection. When <paramref name="caCertPath"/> and <paramref name="clientPfxPath"/>
    /// are both null/empty the connection uses the default OS trust (plain HTTP or a publicly-trusted
    /// host); otherwise a custom <see cref="HttpClient"/> is wired for the private CA and any client cert.
    /// </summary>
    /// <param name="connectionString">The ClickHouse.Client connection string (Host/Port/Protocol/Username/Password/Database).</param>
    /// <param name="caCertPath">Path to a PEM bundle of the private CA (root + intermediate) to trust; null to use OS trust.</param>
    /// <param name="clientPfxPath">Optional path to a PKCS#12 client certificate for mTLS.</param>
    /// <param name="clientPfxPassword">Optional password for <paramref name="clientPfxPath"/>.</param>
    public static ClickHouseConnection Create(
        string connectionString,
        string? caCertPath = null,
        string? clientPfxPath = null,
        string? clientPfxPassword = null)
    {
        if (string.IsNullOrWhiteSpace(caCertPath) && string.IsNullOrWhiteSpace(clientPfxPath))
        {
            return new ClickHouseConnection(connectionString);
        }

        // ClickHouse.Client's built-in HttpClient enables automatic decompression; a custom one must
        // too, or the server's compressed responses arrive un-decompressed.
        var handler = new HttpClientHandler
        {
            AutomaticDecompression = System.Net.DecompressionMethods.All,
        };

        if (!string.IsNullOrWhiteSpace(caCertPath))
        {
            var trustStore = new X509Certificate2Collection();
            trustStore.ImportFromPemFile(caCertPath);
            handler.ServerCertificateCustomValidationCallback = (_, serverCert, _, errors) =>
            {
                if (errors == SslPolicyErrors.None)
                {
                    return true;
                }

                if (serverCert is null)
                {
                    return false;
                }

                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                chain.ChainPolicy.CustomTrustStore.AddRange(trustStore);
                chain.ChainPolicy.ExtraStore.AddRange(trustStore);
                return chain.Build(serverCert);
            };
        }

        if (!string.IsNullOrWhiteSpace(clientPfxPath))
        {
            // HttpClientHandler.ClientCertificateOptions defaults to Manual, so a cert added here is
            // presented during the mTLS handshake.
            handler.ClientCertificates.Add(X509CertificateLoader.LoadPkcs12FromFile(clientPfxPath, clientPfxPassword));
        }

        return new ClickHouseConnection(connectionString, new HttpClient(handler));
    }
}
