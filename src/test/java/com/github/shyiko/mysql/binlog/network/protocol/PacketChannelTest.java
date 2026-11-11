/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.network.protocol;

import com.github.shyiko.mysql.binlog.network.IdentityVerificationException;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PacketChannelTest {

    private ServerSocket serverSocket;
    private Socket clientSocket;

    @BeforeMethod
    public void setUp() throws Exception {
        serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        clientSocket = new Socket("127.0.0.1", serverSocket.getLocalPort());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        clientSocket.close();
        serverSocket.close();
    }

    @Test
    public void upgradeToSSLVerifiesCertificateAgainstConfiguredHost() throws Exception {
        // the host the client was configured with; an IP address in this case
        String configuredHost = "10.0.0.5";
        final AtomicReference<String> verifiedHost = new AtomicReference<String>();

        PacketChannel channel = new PacketChannel(clientSocket);
        channel.upgradeToSSL(sslSocketFactory(new StubSSLSocket(clientSocket.getInetAddress())),
            capturingVerifier(verifiedHost, true), configuredHost);

        assertEquals(verifiedHost.get(), configuredHost);
        assertTrue(channel.isSSL());
    }

    @Test
    public void upgradeToSSLFallsBackToReverseResolvedHostName() throws Exception {
        final AtomicReference<String> verifiedHost = new AtomicReference<String>();

        PacketChannel channel = new PacketChannel(clientSocket);
        channel.upgradeToSSL(sslSocketFactory(new StubSSLSocket(clientSocket.getInetAddress())),
            capturingVerifier(verifiedHost, true), null);

        assertEquals(verifiedHost.get(), clientSocket.getInetAddress().getHostName());
    }

    @Test
    public void upgradeToSSLThrowsIdentityVerificationExceptionWithConfiguredHost() throws Exception {
        String configuredHost = "10.0.0.5";

        PacketChannel channel = new PacketChannel(clientSocket);
        try {
            channel.upgradeToSSL(sslSocketFactory(new StubSSLSocket(clientSocket.getInetAddress())),
                capturingVerifier(new AtomicReference<String>(), false), configuredHost);
            fail("IdentityVerificationException expected");
        } catch (IdentityVerificationException e) {
            assertTrue(e.getMessage().contains(configuredHost));
        }
    }

    private static HostnameVerifier capturingVerifier(final AtomicReference<String> captured,
            final boolean result) {
        return new HostnameVerifier() {
            @Override
            public boolean verify(String host, SSLSession session) {
                captured.set(host);
                return result;
            }
        };
    }

    private static SSLSocketFactory sslSocketFactory(final SSLSocket sslSocket) {
        return new SSLSocketFactory() {
            @Override
            public SSLSocket createSocket(Socket socket) {
                return sslSocket;
            }
        };
    }

    private static class StubSSLSocket extends SSLSocket {

        private final InetAddress address;

        StubSSLSocket(InetAddress address) {
            this.address = address;
        }

        @Override
        public InetAddress getInetAddress() {
            return address;
        }

        @Override
        public InputStream getInputStream() {
            return new java.io.ByteArrayInputStream(new byte[0]);
        }

        @Override
        public OutputStream getOutputStream() {
            return new java.io.ByteArrayOutputStream();
        }

        @Override
        public void startHandshake() {
        }

        @Override
        public SSLSession getSession() {
            return null;
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return new String[0];
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return new String[0];
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) {
        }

        @Override
        public String[] getSupportedProtocols() {
            return new String[0];
        }

        @Override
        public String[] getEnabledProtocols() {
            return new String[0];
        }

        @Override
        public void setEnabledProtocols(String[] protocols) {
        }

        @Override
        public void addHandshakeCompletedListener(HandshakeCompletedListener listener) {
        }

        @Override
        public void removeHandshakeCompletedListener(HandshakeCompletedListener listener) {
        }

        @Override
        public void setUseClientMode(boolean mode) {
        }

        @Override
        public boolean getUseClientMode() {
            return true;
        }

        @Override
        public void setNeedClientAuth(boolean need) {
        }

        @Override
        public boolean getNeedClientAuth() {
            return false;
        }

        @Override
        public void setWantClientAuth(boolean want) {
        }

        @Override
        public boolean getWantClientAuth() {
            return false;
        }

        @Override
        public void setEnableSessionCreation(boolean flag) {
        }

        @Override
        public boolean getEnableSessionCreation() {
            return true;
        }
    }

}
