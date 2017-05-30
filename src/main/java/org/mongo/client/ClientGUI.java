package org.mongo.client;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

/*
 * An alternative MongoClient with its GUI
 */
public class ClientGUI extends JFrame implements ActionListener {

  private static final long serialVersionUID = 1L;
  // to hold "Enter query"
  private JLabel label;
  // to hold the queries
  private JTextField tf;
  // to hold the server address an the port number
  private JTextField tfServer, tfPort, tfDbName;
  // to Logout and get the list of the users
  private JButton connect, disconnect, exit, clear;
  // for the console
  private JTextArea ta;
  // to prettify server response
  private JCheckBox prettifyJson;
  // if it is for connection
  private boolean connected;
  // the Client object
  private Client client;
  // the default port number
  private int defaultPort;
  private String defaultHost, defaultDbName;
  private boolean bPrettifyJson;

  // Constructor connection receiving a socket number
  ClientGUI(String host, int port) {

    super("Alternative Mongo Client [Quering MongoDB via SQL-scripts]");
    defaultPort = port;
    defaultHost = host;
    defaultDbName = "test";

    // The NorthPanel with:
    JPanel northPanel = new JPanel(new GridLayout(3, 1));
    // the server name anmd the port number
    JPanel serverAndPort = new JPanel(new GridLayout(1, 5, 1, 3));
    // the two JTextField with default value for server address and port number
    tfServer = new JTextField(host);
    tfPort = new JTextField("" + port);
    tfPort.setHorizontalAlignment(SwingConstants.RIGHT);
    tfDbName = new JTextField(defaultDbName);

    serverAndPort.add(new JLabel("Server Address:  "));
    serverAndPort.add(tfServer);
    serverAndPort.add(new JLabel("Port Number:  "));
    serverAndPort.add(tfPort);
    serverAndPort.add(new JLabel("DB Name"));
    serverAndPort.add(tfDbName);
    // adds the Server an port field to the GUI
    northPanel.add(serverAndPort);

    // the Label and the TextField
    label = new JLabel("Paste your SQL query here and press <Enter>", SwingConstants.CENTER);
    northPanel.add(label);
    tf = new JTextField("Type SQL query and press <Enter>");
    tf.setBackground(Color.WHITE);
    northPanel.add(tf);
    add(northPanel, BorderLayout.NORTH);

    // The CenterPanel which is the console window
    ta = new JTextArea("", 80, 80);
    JPanel centerPanel = new JPanel(new GridLayout(1, 1));
    centerPanel.add(new JScrollPane(ta));
    ta.setEditable(false);
    add(centerPanel, BorderLayout.CENTER);

    // the buttons
    connect = new JButton("Connect to DB '" + defaultDbName + "'");
    connect.addActionListener(this);
    disconnect = new JButton("Disconnect");
    disconnect.addActionListener(this);
    disconnect.setEnabled(false); // you have to connect before being able to disconnect
    exit = new JButton("Exit");
    exit.addActionListener(this);
    clear = new JButton("Clear");
    clear.addActionListener(this);
    prettifyJson = new JCheckBox("Prettify server response"); // for pretty json format
    prettifyJson.addActionListener(this);

    JPanel southPanel = new JPanel();
    southPanel.add(connect);
    southPanel.add(disconnect);
    southPanel.add(exit);
    southPanel.add(clear);
    southPanel.add(prettifyJson);
    add(southPanel, BorderLayout.SOUTH);

    setDefaultCloseOperation(EXIT_ON_CLOSE);
    setSize(1200, 600);
    setVisible(true);
    tf.requestFocus();

  }

  // called by the Client to append text in the TextArea
  void append(String str) {
    ta.append(str);
    ta.setCaretPosition(ta.getText().length() - 1);
  }

  // called by the GUI is the connection failed
  // we reset our buttons, label, textfield
  void connectionFailed() {
    connect.setEnabled(true);
    disconnect.setEnabled(false);
    // whoIsIn.setEnabled(false);
    label.setText("Enter your username below");
    tf.setText("Anonymous");
    // reset port number and host name as a construction time
    tfPort.setText("" + defaultPort);
    tfServer.setText(defaultHost);
    // let the user change them
    tfServer.setEditable(false);
    tfPort.setEditable(false);
    // don't react to a <CR> after the username
    tf.removeActionListener(this);
    connected = false;
  }

  /*
   * Button or JTextField clicked
   */
  public void actionPerformed(ActionEvent e) {
    Object o = e.getSource();
    // if it is the Prettify Json checkbox
    if (o == prettifyJson) {
      bPrettifyJson = ((JCheckBox) o).isSelected();
      return;
    }
    // if it is the Clear button
    if (o == clear) {
      tf.setText("");
      ta.setText("");
      return;
    }
    // if it is the Disconnect button
    if (o == disconnect) {
      client.disconnect();
      return;
    }
    // if it is the Exit button
    if (o == exit) {
      if (connected) client.disconnect();
      System.exit(0);
    }
    // ok it is coming from the JTextField
    if (connected) {
      // just have to send the query
      client.query(tf.getText());
      tf.setText("");
      return;
    }
    // if it is the 'Connect to DB' button
    if (o == connect) {
      // try creating a new Client with GUI
      client = new Client(this);
      // test if we can start the Client
      if (!client.connect("test"))
        return;
      tf.setText("");
      label.setText("Enter your query below");
      connected = true;

      // disable login button
      connect.setEnabled(false);
      // enable the 2 buttons
      disconnect.setEnabled(true);
      // whoIsIn.setEnabled(true);
      // disable the Server and Port JTextField
      tfServer.setEditable(false);
      tfPort.setEditable(false);
      // Action listener for when the user enter a query
      tf.addActionListener(this);
    }

  }

  public boolean isbPrettifyJson() {
    return bPrettifyJson;
  }

  // to start the whole thing the server
  public static void main(String[] args) {
    new ClientGUI(Utils.HOST, Utils.PORT);
  }

}
